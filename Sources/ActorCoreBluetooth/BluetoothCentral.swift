//
//  BluetoothCentral.swift
//  ActorCoreBluetooth
//
//  Created by Konstantin Polin on 8/1/25.
//  Licensed under the MIT License. See LICENSE file in the project root.
//

import Foundation
import CoreBluetooth

@MainActor
public final class BluetoothCentral {
    private static let powerOnWaitAttempts = 50
    
    private var cbCentralManager: CBCentralManager?
    private var delegateProxy: BluetoothCentralDelegateProxy?
    
    private let logger: BluetoothLogger?
    
    // Connection management
    private var connectionOperations: [UUID: TimedOperation<CBPeripheral>] = [:]
    private var disconnectionOperations: [UUID: TimedOperation<Void>] = [:]
    private var scanOperation: TimedOperation<[DiscoveredPeripheral]>?
    private var discoveredPeripherals: [DiscoveredPeripheral] = []
    
    // Connection state monitoring
    private var connectionStateStreams: [UUID: (continuation: AsyncStream<PeripheralState>.Continuation, peripheralID: UUID)] = [:]
    
    // Connected peripherals registry
    private var connectedPeripherals: [UUID: CBPeripheral] = [:]
    
    public init(logger: BluetoothLogger? = OSLogBluetoothLogger()) {
        self.logger = logger
        logger?.centralInfo("BluetoothCentral initialized")
    }
    
    // MARK: - Central Operations
    
    /// Initialize central manager if needed
    private func ensureCentralManagerInitialized() async throws {
        guard cbCentralManager == nil else {
            logger?.internalDebug("CBCentralManager already initialized")
            return
        }
        
        logger?.centralInfo("Initializing Bluetooth central manager")
        
        let proxy = BluetoothCentralDelegateProxy(central: self, logger: logger)
        self.delegateProxy = proxy
        
        self.cbCentralManager = CBCentralManager(delegate: proxy, queue: DispatchQueue.main)
        
        // Wait for central manager to be ready
        var attempts = 0
        while cbCentralManager?.state != .poweredOn && attempts < Self.powerOnWaitAttempts {
            try await Task.sleep(nanoseconds: 100_000_000) // 0.1 seconds
            attempts += 1
            
            if let state = cbCentralManager?.state {
                logger?.internalDebug("CBCentralManager state check", context: [
                    "state": state.rawValue,
                    "attempt": attempts,
                    "maxAttempts": Self.powerOnWaitAttempts
                ])
                
                // Only throw errors for final states, keep waiting for transitional ones
                switch state {
                case .unknown, .resetting:
                    // These states can resolve themselves, keep waiting
                    continue
                case .poweredOff:
                    logger?.errorError("Bluetooth is powered off")
                    throw BluetoothError.bluetoothPoweredOff
                case .unauthorized:
                    logger?.errorError("Bluetooth access unauthorized")
                    throw BluetoothError.bluetoothUnauthorized
                case .unsupported:
                    logger?.errorError("Bluetooth unsupported on this device")
                    throw BluetoothError.bluetoothUnsupported
                case .poweredOn:
                    // Will exit the while loop naturally
                    break
                @unknown default:
                    // Treat unknown future states as potentially resolvable
                    continue
                }
            }
        }
        
        guard cbCentralManager?.state == .poweredOn else {
            logger?.errorError("Bluetooth failed to become ready after 5 seconds")
            throw BluetoothError.invalidState
        }
        
        logger?.centralNotice("Bluetooth central manager is ready")
    }
    
    /// Scan for peripherals advertising specified services
    public func scanForPeripherals(withServices services: [String]? = nil, timeout: TimeInterval? = nil) async throws -> [DiscoveredPeripheral] {
        try await ensureCentralManagerInitialized()
        
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("CBCentralManager not initialized for scanning")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        // Stop any existing scan first
        if scanOperation != nil {
            logger?.centralInfo("Stopping previous scan to start new one")
            try stopScanning()
        }
        
        let serviceInfo = services?.joined(separator: ", ") ?? "all services"
        let timeoutInfo = timeout.map { "\(Int($0))s" } ?? "no timeout"
        logger?.centralInfo("Starting scan", context: [
            "services": serviceInfo,
            "timeout": timeoutInfo
        ])
        
        return try await withCheckedThrowingContinuation { continuation in
            let scan = TimedOperation<[DiscoveredPeripheral]>(
                operationName: "Scan for peripherals",
                logger: logger
            )
            scan.setup(continuation)
            
            self.scanOperation = scan
            self.discoveredPeripherals = []
            
            if let timeout {
                logger?.internalDebug("Setting scan timeout", context: ["timeout": timeout])
                scan.setTimeoutTask(timeout: timeout) { [weak self] () -> [DiscoveredPeripheral] in
                    guard let self else { return [] }
                    
                    self.logger?.centralNotice("Scan completed (timeout reached)", context: [
                        "timeout": timeout,
                        "discoveredCount": self.discoveredPeripherals.count
                    ])
                    
                    cbCentralManager.stopScan()
                    return self.discoveredPeripherals
                }
            }
            
            let cbServices = services?.compactMap { CBUUID(string: $0) }
            cbCentralManager.scanForPeripherals(withServices: cbServices, options: nil)
            logger?.internalDebug("CBCentralManager.scanForPeripherals started")
        }
    }
    
    /// Stop scanning for peripherals
    public func stopScanning() throws {
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("Cannot stop scanning: CBCentralManager not initialized")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        logger?.centralInfo("Stopping peripheral scan")
        cbCentralManager.stopScan()
        
        // If there's an active scan operation, complete it with current results
        if let scan = scanOperation {
            scanOperation = nil
            let count = discoveredPeripherals.count
            logger?.internalDebug("Completing scan with discovered peripherals", context: ["count": count])
            scan.resumeOnce(with: .success(discoveredPeripherals))
        }
    }
    
    /// Get current central manager state
    public var state: CBManagerState? {
        return cbCentralManager?.state
    }
    
    /// Check if Bluetooth is powered on and ready
    public var isReady: Bool {
        let ready = cbCentralManager?.state == .poweredOn
        logger?.internalDebug("Central manager ready state", context: ["ready": ready])
        return ready
    }

    public func connectToRetrievedPeripherals(withServices: [CBUUID], timeout: TimeInterval? = nil) async throws -> [ConnectedPeripheral] {
        try await ensureCentralManagerInitialized()

        let cbPeripherals = cbCentralManager?.retrieveConnectedPeripherals(withServices: withServices) ?? []
        var connecteds: [ConnectedPeripheral] = []
        for cbPeripheral in cbPeripherals {
            let connected = try await connectToRetrievedPeripheral(cbPeripheral, originalName: cbPeripheral.name, timeout: timeout)
            connecteds.append(connected)
        }
        return connecteds.compactMap({ $0 })
    }

    
    /// Connect to a discovered peripheral
    public func connect(_ peripheral: DiscoveredPeripheral, timeout: TimeInterval? = nil) async throws -> ConnectedPeripheral {
        try await ensureCentralManagerInitialized()
        
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("Cannot connect: CBCentralManager not initialized")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        // Cancel any existing connection attempt for this peripheral
        if let connection = connectionOperations[peripheral.identifier] {
            logger?.connectionInfo("Canceling previous connection attempt to start new one", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            
            // Cancel the old attempt
            cbCentralManager.cancelPeripheralConnection(peripheral.cbPeripheral)
            connection.resumeOnce(with: .failure(BluetoothError.operationCancelled))
            connectionOperations.removeValue(forKey: peripheral.identifier)
        }
        
        // Check for pending disconnection operation
        if disconnectionOperations[peripheral.identifier] != nil {
            logger?.connectionWarning("Cannot connect: disconnection in progress", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            throw BluetoothError.operationInProgress
        }
        
        logger?.logConnection(
            event: "Connecting to peripheral",
            peripheral: peripheral.name ?? "Unknown",
            peripheralID: peripheral.identifier,
            context: timeout.map { ["timeout": $0] }
        )
        
        // Check if already connected
        if peripheral.cbPeripheral.state == .connected {
            logger?.connectionNotice("Peripheral already connected, returning existing connection", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            connectedPeripherals[peripheral.identifier] = peripheral.cbPeripheral
            return ConnectedPeripheral(cbPeripheral: peripheral.cbPeripheral, logger: logger)
        }
        
        let cbPeripheral = try await withCheckedThrowingContinuation { continuation in
            let connection = TimedOperation<CBPeripheral>(
                operationName: "Connect to \(peripheral.identifier)",
                logger: logger
            )
            connection.setup(continuation)
            connectionOperations[peripheral.identifier] = connection
            
            if let timeout {
                logger?.internalDebug("Setting connection timeout", context: [
                    "peripheralID": peripheral.identifier.uuidString,
                    "timeout": timeout
                ])
                connection.setTimeoutTask(timeout: timeout) { [weak self] () -> Void in
                    self?.logger?.logTimeout(
                        operation: "Connection",
                        timeout: timeout,
                        context: ["peripheralID": peripheral.identifier.uuidString]
                    )
                    cbCentralManager.cancelPeripheralConnection(peripheral.cbPeripheral)
                    self?.connectionOperations.removeValue(forKey: peripheral.identifier)
                }
            }
            
            logger?.internalDebug("Calling CBCentralManager.connect", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            cbCentralManager.connect(peripheral.cbPeripheral, options: nil)
        }
        
        logger?.connectionNotice("Successfully connected", context: [
            "peripheralName": peripheral.name ?? "Unknown",
            "peripheralID": peripheral.identifier.uuidString
        ])
        return ConnectedPeripheral(cbPeripheral: cbPeripheral, logger: logger)
    }
    
    /// Connect to a previously connected peripheral for reconnection
    /// Uses the same UUID-based retrieval pattern as the working BluetoothDevice
    public func connect(_ peripheral: ConnectedPeripheral, timeout: TimeInterval? = nil) async throws -> ConnectedPeripheral {
        // Clean up any pending operations from the old ConnectedPeripheral instance
        // This prevents old timeout tasks from firing during/after reconnection
        peripheral.cancelAllPendingOperations()
        
        logger?.internalDebug("Cancelled pending operations from previous connection session", context: [
            "peripheralID": peripheral.identifier.uuidString
        ])
        
        try await ensureCentralManagerInitialized()
        
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("Cannot reconnect: CBCentralManager not initialized")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        // Cancel any existing reconnection attempt for this peripheral ID
        if let connection = connectionOperations[peripheral.identifier] {
            logger?.connectionInfo("Canceling previous reconnection attempt to start new one", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            
            // Cancel at CoreBluetooth level if needed
            if let cbPeripheral = connectedPeripherals[peripheral.identifier] ??
               cbCentralManager.retrievePeripherals(withIdentifiers: [peripheral.identifier]).first {
                cbCentralManager.cancelPeripheralConnection(cbPeripheral)
            }
            
            connection.resumeOnce(with: .failure(BluetoothError.operationCancelled))
            connectionOperations.removeValue(forKey: peripheral.identifier)
        }
        
        // Check for pending disconnection operation
        if disconnectionOperations[peripheral.identifier] != nil {
            logger?.connectionWarning("Cannot reconnect: disconnection in progress", context: [
                "peripheralID": peripheral.identifier.uuidString
            ])
            throw BluetoothError.operationInProgress
        }
        
        let peripheralID = peripheral.identifier
        logger?.connectionInfo("Attempting reconnection", context: [
            "peripheralName": peripheral.name ?? "Unknown",
            "peripheralID": peripheralID.uuidString,
            "currentState": peripheral.state,
            "timeout": timeout as Any
        ])
        
        // Reconnection Strategy 1: Check if it's still in our connected peripherals registry
        if let cbPeripheral = connectedPeripherals[peripheralID] {
            logger?.internalDebug("Found peripheral in connected registry", context: [
                "peripheralID": peripheralID.uuidString,
                "cbState": cbPeripheral.state.rawValue
            ])
            
            // Even if we think it's connected, treat as reconnection for consistency
            return try await connectToRetrievedPeripheral(cbPeripheral, originalName: peripheral.name, timeout: timeout)
        }
        
        // Reconnection Strategy 2: Check if it's in discovered peripherals cache
        if let discovered = discoveredPeripherals.first(where: { $0.identifier == peripheralID }) {
            logger?.internalDebug("Found peripheral in discovered cache", context: [
                "peripheralID": peripheralID.uuidString
            ])
            return try await connect(discovered, timeout: timeout)
        }
        
        // Reconnection Strategy 3: Use CoreBluetooth's retrievePeripherals (BluetoothDevice pattern)
        logger?.internalDebug("Using CBCentralManager.retrievePeripherals for reconnection", context: [
            "peripheralID": peripheralID.uuidString
        ])
        
        let retrievedPeripherals = cbCentralManager.retrievePeripherals(withIdentifiers: [peripheralID])
        guard let cbPeripheral = retrievedPeripherals.first else {
            logger?.errorWarning("Peripheral not found for reconnection", context: [
                "peripheralID": peripheralID.uuidString,
                "retrievedCount": retrievedPeripherals.count
            ])
            throw BluetoothError.peripheralNotFound
        }
        
        logger?.internalDebug("Successfully retrieved peripheral via CoreBluetooth", context: [
            "peripheralID": peripheralID.uuidString,
            "peripheralName": cbPeripheral.name ?? "Unknown",
            "cbState": cbPeripheral.state.rawValue
        ])
        
        return try await connectToRetrievedPeripheral(cbPeripheral, originalName: peripheral.name, timeout: timeout)
    }
    
    /// Disconnect from any connected peripheral
    public func disconnect(_ peripheralID: UUID, timeout: TimeInterval? = nil) async throws {
        try await ensureCentralManagerInitialized()
        
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("Cannot disconnect: CBCentralManager not initialized")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        guard let cbPeripheral = connectedPeripherals[peripheralID] else {
            logger?.errorWarning("Peripheral not found in connected peripherals", context: [
                "peripheralID": peripheralID.uuidString
            ])
            throw BluetoothError.peripheralNotFound
        }
        
        // Check for existing disconnection operation
        if disconnectionOperations[peripheralID] != nil {
            logger?.connectionWarning("Cannot disconnect: disconnection already in progress", context: [
                "peripheralID": peripheralID.uuidString
            ])
            throw BluetoothError.operationInProgress
        }
        
        // Check if already disconnected
        if cbPeripheral.state == .disconnected {
            logger?.connectionNotice("Peripheral already disconnected", context: [
                "peripheralID": peripheralID.uuidString
            ])
            // Clean up our registry and return immediately
            connectedPeripherals.removeValue(forKey: peripheralID)
            return
        }
        
        logger?.logConnection(
            event: "Disconnecting from peripheral",
            peripheral: cbPeripheral.name ?? "Unknown",
            peripheralID: peripheralID,
            context: timeout.map { ["timeout": $0] }
        )
        
        try await withCheckedThrowingContinuation { continuation in
            let disconnection = TimedOperation<Void>(
                operationName: "Disconnect from \(peripheralID)",
                logger: logger
            )
            disconnection.setup(continuation)
            disconnectionOperations[peripheralID] = disconnection
            
            if let timeout {
                logger?.internalDebug("Setting disconnection timeout", context: [
                    "peripheralID": peripheralID.uuidString,
                    "timeout": timeout
                ])
                disconnection.setTimeoutTask(timeout: timeout, onTimeout: { [weak self] () -> Void in
                    self?.logger?.logTimeout(
                        operation: "Disconnection",
                        timeout: timeout,
                        context: ["peripheralID": peripheralID.uuidString]
                    )
                    self?.disconnectionOperations.removeValue(forKey: peripheralID)
                })
            }
            
            logger?.internalDebug("Calling CBCentralManager.cancelPeripheralConnection", context: [
                "peripheralID": peripheralID.uuidString
            ])
            cbCentralManager.cancelPeripheralConnection(cbPeripheral)
        }
        
        logger?.connectionNotice("Successfully disconnected", context: [
            "peripheralName": cbPeripheral.name ?? "Unknown",
            "peripheralID": peripheralID.uuidString
        ])
    }
    
    /// Disconnect using connected peripheral object
    public func disconnect(_ peripheral: ConnectedPeripheral, timeout: TimeInterval? = nil) async throws {
        logger?.internalDebug("Disconnecting peripheral via ConnectedPeripheral object")
        try await disconnect(peripheral.identifier, timeout: timeout)
    }
    
    /// Get list of currently connected peripheral IDs
    public var connectedPeripheralIDs: [UUID] {
        let ids = Array(connectedPeripherals.keys)
        logger?.internalDebug("Currently connected peripherals", context: ["count": ids.count])
        return ids
    }
    
    /// Check if a specific peripheral is connected
    public func isConnected(_ peripheralID: UUID) -> Bool {
        let connected = connectedPeripherals[peripheralID]?.state == .connected
        logger?.internalDebug("Peripheral connection status", context: [
            "peripheralID": peripheralID.uuidString,
            "connected": connected
        ])
        return connected
    }
    
    /// Scan and connect in one operation
    public func scanAndConnect(
        withServices services: [String]? = nil,
        scanTimeout: TimeInterval? = 5.0,
        connectTimeout: TimeInterval? = 10.0,
        filter: ((DiscoveredPeripheral) -> Bool)? = nil
    ) async throws -> ConnectedPeripheral {
        logger?.centralInfo("One-shot scan and connect operation")
        
        // Scan for devices
        let discovered = try await scanForPeripherals(withServices: services, timeout: scanTimeout)
        logger?.internalDebug("Scan completed, applying filter", context: [
            "discoveredCount": discovered.count
        ])
        
        // Apply filter if provided, otherwise use first device
        guard let targetDevice = filter != nil ? discovered.first(where: filter!) : discovered.first else {
            logger?.errorWarning("No suitable peripheral found after scan")
            throw BluetoothError.peripheralNotFound
        }
        
        logger?.centralInfo("Selected peripheral for connection", context: [
            "peripheralName": targetDevice.name ?? "Unknown",
            "peripheralID": targetDevice.identifier.uuidString
        ])
        
        // Connect to device
        return try await connect(targetDevice, timeout: connectTimeout)
    }
    
    // MARK: - Connection State Monitoring
    
    /// Create monitor for connection state changes
    public func createConnectionStateMonitor(for peripheralID: UUID) -> (stream: AsyncStream<PeripheralState>, monitorID: UUID) {
        let monitorID = UUID()
        
        logger?.streamInfo("Creating connection state monitor", context: [
            "peripheralID": peripheralID.uuidString,
            "monitorID": monitorID.uuidString
        ])
        
        let stream = AsyncStream<PeripheralState> { continuation in
            connectionStateStreams[monitorID] = (continuation, peripheralID)
        }
        
        return (stream, monitorID)
    }
    
    /// Stop monitoring connection state
    public func stopConnectionStateMonitoring(_ monitorID: UUID) {
        logger?.streamInfo("Stopping connection state monitor", context: [
            "monitorID": monitorID.uuidString
        ])
        connectionStateStreams[monitorID]?.continuation.finish()
        connectionStateStreams.removeValue(forKey: monitorID)
    }
    
    /// Convenience method for monitoring connection state with automatic cleanup
    public func withConnectionStateMonitoring<T: Sendable>(
        for peripheralID: UUID,
        operation: (AsyncStream<PeripheralState>) async throws -> T
    ) async rethrows -> T {
        logger?.streamInfo("Starting connection state monitoring with auto-cleanup", context: [
            "peripheralID": peripheralID.uuidString
        ])
        
        let (stream, monitorID) = createConnectionStateMonitor(for: peripheralID)
        
        defer {
            stopConnectionStateMonitoring(monitorID)
            logger?.internalDebug("Auto-cleanup completed", context: [
                "monitorID": monitorID.uuidString
            ])
        }
        
        return try await operation(stream)
    }
    
    /// Disconnect all peripherals and clean up resources
    public func disconnectAll() async throws {
        logger?.centralInfo("Cleaning up BluetoothCentral resources")
        
        // Stop any active scanning
        try? stopScanning()
        
        // Disconnect all connected peripherals
        let peripheralCount = connectedPeripheralIDs.count
        if peripheralCount > 0 {
            logger?.centralInfo("Disconnecting connected peripherals", context: ["count": peripheralCount])
            for peripheralID in connectedPeripheralIDs {
                try? await disconnect(peripheralID)
            }
        }
        
        // Clean up streams
        let streamCount = connectionStateStreams.count
        if streamCount > 0 {
            logger?.streamInfo("Cleaning up connection state streams", context: ["count": streamCount])
            for (monitorID, _) in connectionStateStreams {
                stopConnectionStateMonitoring(monitorID)
            }
        }
        
        // Clean up any pending operations
        let pendingConnections = connectionOperations.count
        let pendingDisconnections = disconnectionOperations.count
        
        if pendingConnections > 0 {
            logger?.internalDebug("Cleaning up pending connection operations", context: ["count": pendingConnections])
            for (_, operation) in connectionOperations {
                operation.resumeOnce(with: .failure(BluetoothError.operationCancelled))
            }
            connectionOperations.removeAll()
        }
        
        if pendingDisconnections > 0 {
            logger?.internalDebug("Cleaning up pending disconnection operations", context: ["count": pendingDisconnections])
            for (_, operation) in disconnectionOperations {
                operation.resumeOnce(with: .failure(BluetoothError.operationCancelled))
            }
            disconnectionOperations.removeAll()
        }
        
        // Clear peripheral registry
        connectedPeripherals.removeAll()
        
        logger?.centralNotice("BluetoothCentral cleanup completed")
    }
    
    // MARK: - Internal Delegate Handling Methods
    
    /// Handle peripheral state changes, managing both operation completion and state monitoring
    internal func handlePeripheralStateChange(for peripheralID: UUID, newState: PeripheralState, cbPeripheral: CBPeripheral, error: Error? = nil) {
        
        logger?.connectionInfo("Peripheral state changed", context: [
            "peripheralName": cbPeripheral.name ?? "Unknown",
            "peripheralID": peripheralID.uuidString,
            "newState": newState.debugDescription,
            "error": error?.localizedDescription as Any
        ])
        
        // Update internal state tracking
        switch newState {
        case .connected:
            connectedPeripherals[peripheralID] = cbPeripheral
            logger?.internalDebug("Registered peripheral in connected registry", context: [
                "peripheralID": peripheralID.uuidString
            ])
            
            if let connection = connectionOperations[peripheralID] {
                logger?.internalDebug("Found pending connection operation", context: [
                    "peripheralID": peripheralID.uuidString,
                    "state": newState.debugDescription
                ])
                connectionOperations.removeValue(forKey: peripheralID)
                
                logger?.connectionNotice("Completing connection operation with success", context: [
                    "peripheralName": cbPeripheral.name ?? "Unknown",
                    "peripheralID": peripheralID.uuidString
                ])
                connection.resumeOnce(with: .success(cbPeripheral))
            }
        
        case .disconnected:
            let wasConnected = connectedPeripherals[peripheralID] != nil
            connectedPeripherals.removeValue(forKey: peripheralID)
            if wasConnected {
                logger?.internalDebug("Removed peripheral from connected registry", context: [
                    "peripheralID": peripheralID.uuidString
                ])
            }
            
            // Handle pending disconnection operations (successful disconnection)
            if let disconnection = disconnectionOperations[peripheralID] {
                logger?.internalDebug("Found pending disconnection operation", context: [
                    "peripheralID": peripheralID.uuidString,
                    "state": newState.debugDescription
                ])
                disconnectionOperations.removeValue(forKey: peripheralID)
                
                logger?.connectionNotice("Completing disconnection operation with success", context: [
                    "peripheralName": cbPeripheral.name ?? "Unknown",
                    "peripheralID": peripheralID.uuidString
                ])
                disconnection.resumeOnce(with: .success(()))
            }
            
            // Handle pending connection operations (failed connection)
            if let connection = connectionOperations[peripheralID] {
                // If there was a pending connection and we got disconnected, it failed
                let connectionError = error ?? BluetoothError.connectionFailed
                logger?.connectionWarning("Completing connection operation with failure", context: [
                    "peripheralName": cbPeripheral.name ?? "Unknown",
                    "peripheralID": peripheralID.uuidString,
                    "error": connectionError.localizedDescription
                ])
                connectionOperations.removeValue(forKey: peripheralID)
                connection.resumeOnce(with: .failure(connectionError))
            }
            
        case .connecting, .disconnecting:
            logger?.internalDebug("Transitional state - no registry changes", context: [
                "peripheralID": peripheralID.uuidString,
                "state": newState.debugDescription
            ])
        }
        
        // Notify state monitors
        let monitorCount = connectionStateStreams.values.count { $0.peripheralID == peripheralID }
        if monitorCount > 0 {
            logger?.streamDebug("Notifying connection state monitors", context: [
                "peripheralID": peripheralID.uuidString,
                "monitorCount": monitorCount,
                "newState": newState.debugDescription
            ])
            
            for (monitorID, (continuation, monitoredPeripheralID)) in connectionStateStreams {
                if monitoredPeripheralID == peripheralID {
                    continuation.yield(newState)
                    logger?.internalDebug("Notified monitor", context: [
                        "monitorID": monitorID.uuidString,
                        "peripheralID": peripheralID.uuidString,
                        "state": newState.debugDescription
                    ])
                }
            }
        } else {
            logger?.internalDebug("No monitors to notify for peripheral", context: [
                "peripheralID": peripheralID.uuidString
            ])
        }
        
        // Log final state
        let connectedCount = connectedPeripherals.count
        let monitoringCount = connectionStateStreams.count
        let pendingConnectionsCount = connectionOperations.count
        let pendingDisconnectionsCount = disconnectionOperations.count
        
        logger?.internalDebug("State change processing completed", context: [
            "peripheralID": peripheralID.uuidString,
            "finalState": newState.debugDescription,
            "connectedPeripherals": connectedCount,
            "activeMonitors": monitoringCount,
            "pendingConnections": pendingConnectionsCount,
            "pendingDisconnections": pendingDisconnectionsCount
        ])
    }
    
     
    // Called by delegate proxy when peripheral is discovered
    internal func handlePeripheralDiscovered(_ peripheral: CBPeripheral, advertisementData: [String: Any], rssi: NSNumber) {
        let discoveredPeripheral = DiscoveredPeripheral(
            cbPeripheral: peripheral,
            advertisementData: advertisementData,
            rssi: rssi
        )
        
        // Add to discovered peripherals if not already present
        if !discoveredPeripherals.contains(where: { $0.identifier == peripheral.identifier }) {
            discoveredPeripherals.append(discoveredPeripheral)
            logger?.centralDebug("Discovered new peripheral", context: [
                "peripheralName": peripheral.name ?? "Unknown",
                "peripheralID": peripheral.identifier.uuidString,
                "rssi": rssi.intValue
            ])
        } else {
            logger?.internalDebug("Updated existing peripheral", context: [
                "peripheralName": peripheral.name ?? "Unknown",
                "peripheralID": peripheral.identifier.uuidString,
                "rssi": rssi.intValue
            ])
        }
    }
    
    /// Helper method to connect to a retrieved CBPeripheral
    private func connectToRetrievedPeripheral(_ cbPeripheral: CBPeripheral, originalName: String?, timeout: TimeInterval?) async throws -> ConnectedPeripheral {
        guard let cbCentralManager = cbCentralManager else {
            logger?.errorError("CBCentralManager not initialized for scanning")
            throw BluetoothError.centralManagerNotInitialized
        }
        
        logger?.logConnection(
            event: "Reconnecting to retrieved peripheral",
            peripheral: cbPeripheral.name ?? originalName ?? "Unknown",
            peripheralID: cbPeripheral.identifier,
            context: timeout.map { ["timeout": $0] }
        )
        
        // Check if already connected at CoreBluetooth level
        if cbPeripheral.state == .connected {
            logger?.connectionNotice("Peripheral already connected at CB level, registering with library", context: [
                "peripheralID": cbPeripheral.identifier.uuidString
            ])
            connectedPeripherals[cbPeripheral.identifier] = cbPeripheral
            return ConnectedPeripheral(cbPeripheral: cbPeripheral, logger: logger)
        }
        
        let connectedPeripheral = try await withCheckedThrowingContinuation { continuation in
            let connection = TimedOperation<CBPeripheral>(
                operationName: "Reconnect to \(cbPeripheral.identifier)",
                logger: logger
            )
            connection.setup(continuation)
            connectionOperations[cbPeripheral.identifier] = connection
            
            if let timeout {
                logger?.internalDebug("Setting reconnection timeout", context: [
                    "peripheralID": cbPeripheral.identifier.uuidString,
                    "timeout": timeout
                ])
                connection.setTimeoutTask(timeout: timeout) { [weak self] () -> Void in
                    self?.logger?.logTimeout(
                        operation: "Reconnection",
                        timeout: timeout,
                        context: ["peripheralID": cbPeripheral.identifier.uuidString]
                    )
                    cbCentralManager.cancelPeripheralConnection(cbPeripheral)
                    self?.connectionOperations.removeValue(forKey: cbPeripheral.identifier)
                }
            }
            
            logger?.internalDebug("Calling CBCentralManager.connect for reconnection", context: [
                "peripheralID": cbPeripheral.identifier.uuidString
            ])
            cbCentralManager.connect(cbPeripheral, options: nil)
        }
        
        logger?.connectionNotice("Successfully reconnected", context: [
            "peripheralName": cbPeripheral.name ?? originalName ?? "Unknown",
            "peripheralID": cbPeripheral.identifier.uuidString
        ])
        
        connectedPeripherals[cbPeripheral.identifier] = cbPeripheral
        logger?.internalDebug("Updated peripheral registry for future reconnections", context: [
            "peripheralID": cbPeripheral.identifier.uuidString,
            "peripheralName": cbPeripheral.name ?? "Unknown"
        ])
        
        return ConnectedPeripheral(cbPeripheral: connectedPeripheral, logger: logger)
    }
}

// MARK: - Central Delegate Proxy

/// Thin proxy for handling CB delegate callbacks - only central-level events
@MainActor
private final class BluetoothCentralDelegateProxy: NSObject, @preconcurrency CBCentralManagerDelegate {
    
    private weak var central: BluetoothCentral?
    private let logger: BluetoothLogger?
    
    init(central: BluetoothCentral, logger: BluetoothLogger?) {
        self.central = central
        self.logger = logger
        super.init()
        logger?.internalDebug("BluetoothCentralDelegateProxy initialized")
    }
    
    // MARK: - CBCentralManagerDelegate
    
    func centralManagerDidUpdateState(_ central: CBCentralManager) {
        let stateDescription: String
        switch central.state {
        case .unknown: stateDescription = "unknown"
        case .resetting: stateDescription = "resetting"
        case .unsupported: stateDescription = "unsupported"
        case .unauthorized: stateDescription = "unauthorized"
        case .poweredOff: stateDescription = "poweredOff"
        case .poweredOn: stateDescription = "poweredOn"
        @unknown default: stateDescription = "unknown(\(central.state.rawValue))"
        }
        
        logger?.centralInfo("Central manager state updated", context: [
            "state": stateDescription
        ])
    }
    
    func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        logger?.logConnection(
            event: "Connected",
            peripheral: peripheral.name ?? "Unknown",
            peripheralID: peripheral.identifier
        )
        
        self.central?.handlePeripheralStateChange(
            for: peripheral.identifier,
            newState: .connected,
            cbPeripheral: peripheral
        )
    }
    
    func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        let errorDescription = error?.localizedDescription ?? "Unknown error"
        logger?.logConnection(
            event: "Connection failed",
            peripheral: peripheral.name ?? "Unknown",
            peripheralID: peripheral.identifier,
            context: ["error": errorDescription]
        )
        
        // Connection failure means we go to disconnected state with error context
        self.central?.handlePeripheralStateChange(
            for: peripheral.identifier,
            newState: .disconnected,
            cbPeripheral: peripheral,
            error: error
        )
    }
    
    func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        if let error = error {
            logger?.connectionWarning("Peripheral disconnected with error", context: [
                "peripheralName": peripheral.name ?? "Unknown",
                "peripheralID": peripheral.identifier.uuidString,
                "error": error.localizedDescription
            ])
        } else {
            logger?.logConnection(
                event: "Disconnected",
                peripheral: peripheral.name ?? "Unknown",
                peripheralID: peripheral.identifier
            )
        }
        
        self.central?.handlePeripheralStateChange(
            for: peripheral.identifier,
            newState: .disconnected,
            cbPeripheral: peripheral,
            error: error
        )
    }
    
    func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral, advertisementData: [String: Any], rssi RSSI: NSNumber) {
        logger?.internalDebug("CBCentralManagerDelegate.didDiscover called", context: [
            "peripheralName": peripheral.name ?? "Unknown",
            "peripheralID": peripheral.identifier.uuidString,
            "rssi": RSSI.intValue
        ])
        
        self.central?.handlePeripheralDiscovered(peripheral, advertisementData: advertisementData, rssi: RSSI)
    }
}
