import Cocoa
import Darwin
import Foundation
import os

enum Direction {
    case next
    case prev

    var value: String {
        switch self {
        case .next:
            "next"
        case .prev:
            "prev"
        }
    }
}

enum GestureState {
    case began
    case ended
}

enum SwipeError: Error {
    case SocketError(String)
    case CommandFail(String)
    case Unknown(String)
}

private struct WorkspaceSwitchSettings {
    let wrapWorkspace: Bool
    let skipEmpty: Bool
    let keyboardOrder: Bool
}

private struct GestureSettings {
    let swipeThreshold: Float
    let naturalSwipe: Bool
    let fingerCount: Int
}

private enum SettingKey {
    static let threshold = "threshold"
    static let wrap = "wrap"
    static let natural = "natrual"
    static let skipEmpty = "skip-empty"
    static let keyboardOrder = "qwerty-flow"
    static let fingers = "fingers"
}

public struct ClientRequest: Codable, Sendable {
    public let command: String
    public let args: [String]
    public let stdin: String

    public init(
        args: [String],
        stdin: String
    ) {
        self.command = ""
        self.args = args
        self.stdin = stdin
    }
}

public struct ServerAnswer: Codable, Sendable {
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String
    public let serverVersionAndHash: String

    public init(
        exitCode: Int32,
        stdout: String = "",
        stderr: String = "",
        serverVersionAndHash: String
    ) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
        self.serverVersionAndHash = serverVersionAndHash
    }
}

private final class AeroSpaceSocket {
    private static let bufferSize = 4096
    private var readBuffer = [UInt8](
        repeating: 0,
        count: AeroSpaceSocket.bufferSize
    )
    private var descriptor: Int32

    init(path: String) throws {
        descriptor = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else {
            throw Self.posixError()
        }

        do {
            try connect(path: path)
            try setNonBlocking()
        } catch {
            close()
            throw error
        }
    }

    deinit {
        close()
    }

    func close() {
        if descriptor >= 0 {
            Darwin.close(descriptor)
            descriptor = -1
        }
    }

    func write(_ data: Data) throws {
        try data.withUnsafeBytes { rawBuffer in
            guard let baseAddress = rawBuffer.baseAddress else { return }
            var offset = 0

            while offset < rawBuffer.count {
                let count = Darwin.write(
                    descriptor,
                    baseAddress.advanced(by: offset),
                    rawBuffer.count - offset
                )

                if count > 0 {
                    offset += count
                } else if errno == EINTR {
                    continue
                } else if errno == EAGAIN || errno == EWOULDBLOCK {
                    try wait(for: Int16(POLLOUT))
                } else {
                    throw Self.posixError()
                }
            }
        }
    }

    func readResponse(using decoder: JSONDecoder) throws -> ServerAnswer {
        var responseData = Data()
        responseData.reserveCapacity(Self.bufferSize)

        while true {
            try wait(for: Int16(POLLIN))
            try readAvailable(into: &responseData)

            if let response = try? decoder.decode(
                ServerAnswer.self,
                from: responseData
            ) {
                return response
            }
        }
    }

    private func connect(path: String) throws {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)

        let pathBytes = path.utf8CString
        guard pathBytes.count <= MemoryLayout.size(ofValue: address.sun_path)
        else {
            throw POSIXError(.ENAMETOOLONG)
        }

        withUnsafeMutableBytes(of: &address.sun_path) { buffer in
            buffer.copyBytes(from: pathBytes.map { UInt8(bitPattern: $0) })
        }

        let addressLength = socklen_t(
            MemoryLayout<sockaddr_un>.offset(of: \.sun_path)! + pathBytes.count
        )
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, addressLength)
            }
        }

        guard result == 0 else {
            throw Self.posixError()
        }
    }

    private func setNonBlocking() throws {
        let flags = fcntl(descriptor, F_GETFL, 0)
        guard flags >= 0 else {
            throw Self.posixError()
        }
        guard fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) >= 0 else {
            throw Self.posixError()
        }
    }

    private func wait(for events: Int16) throws {
        var fileDescriptor = pollfd(fd: descriptor, events: events, revents: 0)

        while Darwin.poll(&fileDescriptor, 1, -1) < 0 {
            if errno == EINTR {
                continue
            }
            throw Self.posixError()
        }

        let failureEvents =
            Int16(POLLERR) | Int16(POLLHUP) | Int16(POLLNVAL)
        if fileDescriptor.revents & failureEvents != 0 {
            throw POSIXError(.ECONNRESET)
        }
    }

    private func readAvailable(into data: inout Data) throws {
        while true {
            let count = readBuffer.withUnsafeMutableBytes {
                Darwin.read(descriptor, $0.baseAddress, $0.count)
            }

            if count > 0 {
                data.append(readBuffer, count: count)
            } else if count == 0 {
                throw POSIXError(.ECONNRESET)
            } else if errno == EINTR {
                continue
            } else if errno == EAGAIN || errno == EWOULDBLOCK {
                return
            } else {
                throw Self.posixError()
            }
        }
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}

final class SocketInfo: ObservableObject {
    @Published var socketConnected: Bool = false
}

final class SwipeManager {
    var socketInfo = SocketInfo()

    private var eventTap: CFMachPort? = nil
    private var accDisX: Float = 0
    private var prevTouchPositions: [ObjectIdentifier: NSPoint] = [:]
    private var state: GestureState = .ended
    private var gestureSettings = Self.readGestureSettings()
    private var workspaceSwitchSettings = Self.readWorkspaceSwitchSettings()
    private var settingsObserver: NSObjectProtocol? = nil
    private var socket: AeroSpaceSocket? = nil
    private let jsonEncoder = JSONEncoder()
    private let jsonDecoder = JSONDecoder()
    private let commandQueueKey = DispatchSpecificKey<Void>()
    private let commandQueue = DispatchQueue(
        label: "club.mediosz.SwipeAeroSpace.commands",
        qos: .userInitiated
    )

    private static let keyboardOrderedWorkspaces = Array(
        "123456789QWERTYUIOPASDFGHJKLZXCVBNM"
    ).map { String($0) }
    private static let keyboardOrderedWorkspacePositions = Dictionary(
        uniqueKeysWithValues: keyboardOrderedWorkspaces.enumerated().map {
            ($0.element, $0.offset)
        }
    )

    private static func readGestureSettings() -> GestureSettings {
        let defaults = UserDefaults.standard
        let threshold =
            defaults.object(forKey: SettingKey.threshold) as? Double ?? 0.3
        let naturalSwipe = boolSetting(
            SettingKey.natural,
            defaultValue: true
        )
        let fingerCount =
            defaults.string(forKey: SettingKey.fingers) == "Four" ? 4 : 3

        return GestureSettings(
            swipeThreshold: Float(threshold),
            naturalSwipe: naturalSwipe,
            fingerCount: fingerCount
        )
    }

    private static func readWorkspaceSwitchSettings()
        -> WorkspaceSwitchSettings
    {
        WorkspaceSwitchSettings(
            wrapWorkspace: boolSetting(SettingKey.wrap, defaultValue: false),
            skipEmpty: boolSetting(SettingKey.skipEmpty, defaultValue: false),
            keyboardOrder: boolSetting(
                SettingKey.keyboardOrder,
                defaultValue: false
            )
        )
    }

    private static func boolSetting(
        _ key: String,
        defaultValue: Bool
    ) -> Bool {
        let defaults = UserDefaults.standard
        guard defaults.object(forKey: key) != nil else {
            return defaultValue
        }
        return defaults.bool(forKey: key)
    }

    private var logger: Logger = Logger(
        subsystem: Bundle.main.bundleIdentifier!,
        category: "Info"
    )

    init() {
        commandQueue.setSpecific(key: commandQueueKey, value: ())
        settingsObserver = NotificationCenter.default.addObserver(
            forName: UserDefaults.didChangeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.reloadSettings()
        }
    }

    deinit {
        if let settingsObserver {
            NotificationCenter.default.removeObserver(settingsObserver)
        }
    }

    private func reloadSettings() {
        gestureSettings = Self.readGestureSettings()
        workspaceSwitchSettings = Self.readWorkspaceSwitchSettings()
    }

    private func runCommand(args: [String], stdin: String, retry: Bool = false)
        -> Result<String, SwipeError>
    {
        guard let socket = socket else {
            return .failure(.SocketError("No socket created"))
        }
        do {
            let request = try jsonEncoder.encode(
                ClientRequest(args: args, stdin: stdin)
            )
            try socket.write(request)
            let result = try socket.readResponse(using: jsonDecoder)
            if result.exitCode != 0 {
                return .failure(.CommandFail(result.stderr))
            }
            return .success(result.stdout)

        } catch let error {
            // Reconnect once; the AeroSpace socket can disappear during restarts.
            if retry {
                return .failure(.SocketError(error.localizedDescription))
            }
            logger.info("Trying reconnect socket...")
            connectSocketNow(reconnect: true)
            return runCommand(args: args, stdin: stdin, retry: true)
        }
    }

    private func getNonEmptyWorkspaces() -> Result<String, SwipeError> {
        let args = [
            "list-workspaces", "--monitor", "focused", "--empty", "no",
        ]
        return runCommand(args: args, stdin: "")
    }

    @discardableResult
    private func switchWorkspace(
        direction: Direction,
        settings: WorkspaceSwitchSettings
    ) -> Result<
        String, SwipeError
    > {

        var res = runCommand(
            args: ["list-workspaces", "--monitor", "mouse", "--visible"],
            stdin: ""
        )
        guard let mouse_on = try? res.get() else {
            return res
        }

        let currentWorkspace = mouse_on.trimmingCharacters(
            in: .whitespacesAndNewlines
        ).uppercased()

        res = runCommand(args: ["workspace", currentWorkspace], stdin: "")
        guard (try? res.get()) != nil else {
            return res
        }

        var args = ["workspace", direction.value]
        if settings.wrapWorkspace {
            args.append("--wrap-around")
        }
        var stdin = ""

        var nonEmptyWorkspaces: Set<String> = []
        if settings.skipEmpty {
            res = getNonEmptyWorkspaces()
            guard let ws = try? res.get() else {
                return res
            }
            stdin = ws
            nonEmptyWorkspaces = Set(
                ws.split(separator: "\n")
                    .map {
                        $0.trimmingCharacters(
                            in: .whitespacesAndNewlines
                        ).uppercased()
                    }
                    .filter { !$0.isEmpty }
            )
        }

        if !settings.keyboardOrder {
            return runCommand(args: args, stdin: stdin)
        } else {
            let orderedWorkspaces = Self.keyboardOrderedWorkspaces

            guard
                let currentPosition =
                    Self.keyboardOrderedWorkspacePositions[currentWorkspace]
            else {
                return runCommand(args: args, stdin: stdin)
            }

            // Hàm tìm workspace tiếp theo (có thể bỏ qua empty workspace)
            func findNextValidWorkspace(
                from position: Int,
                direction: Direction
            ) -> Int? {
                let totalCount = orderedWorkspaces.count
                var searchPosition = position
                var attempts = 0

                while attempts < totalCount {
                    switch direction {
                    case .next:
                        searchPosition += 1
                        if settings.wrapWorkspace && searchPosition >= totalCount {
                            searchPosition = 0
                        } else if searchPosition >= totalCount {
                            return nil
                        }
                    case .prev:
                        searchPosition -= 1
                        if settings.wrapWorkspace && searchPosition < 0 {
                            searchPosition = totalCount - 1
                        } else if searchPosition < 0 {
                            return nil
                        }
                    default:
                        return nil
                    }

                    let targetWorkspace = orderedWorkspaces[searchPosition]

                    if !settings.skipEmpty
                        || nonEmptyWorkspaces.contains(targetWorkspace)
                    {
                        return searchPosition
                    }

                    attempts += 1
                }

                return nil
            }

            guard
                let targetPosition = findNextValidWorkspace(
                    from: currentPosition,
                    direction: direction
                )
            else {
                return .failure(.Unknown("No valid workspace found"))
            }

            let targetWorkspace = orderedWorkspaces[targetPosition]

            return runCommand(args: ["workspace", targetWorkspace], stdin: stdin)
        }
    }

    func nextWorkspace() {
        switchWorkspaceAsync(
            direction: .next,
            settings: currentWorkspaceSwitchSettings()
        )
    }

    func prevWorkspace() {
        switchWorkspaceAsync(
            direction: .prev,
            settings: currentWorkspaceSwitchSettings()
        )
    }

    private func switchWorkspaceAsync(
        direction: Direction,
        settings: WorkspaceSwitchSettings
    ) {
        commandQueue.async { [weak self] in
            guard let self else { return }
            switch self.switchWorkspace(direction: direction, settings: settings) {
            case .success: return
            case .failure(let err): self.logger.error("\(err.localizedDescription)")
            }
        }
    }

    func connectSocket(reconnect: Bool = false) {
        if DispatchQueue.getSpecific(key: commandQueueKey) != nil {
            connectSocketNow(reconnect: reconnect)
            return
        }

        commandQueue.async { [weak self] in
            self?.connectSocketNow(reconnect: reconnect)
        }
    }

    private func connectSocketNow(reconnect: Bool = false) {
        if socket != nil && !reconnect {
            logger.warning("socket is connected")
            return
        }

        if reconnect {
            socket?.close()
            socket = nil
            setSocketConnected(false)
        }

        let socket_path = "/tmp/bobko.aerospace-\(NSUserName()).sock"
        do {
            let connectedSocket = try AeroSpaceSocket(path: socket_path)
            socket = connectedSocket
            setSocketConnected(true)
            logger.info("connect to socket \(socket_path)")
        } catch let error {
            socket?.close()
            socket = nil
            setSocketConnected(false)
            logger.error("Unexpected error: \(error.localizedDescription)")
        }
    }

    private func setSocketConnected(_ isConnected: Bool) {
        if Thread.isMainThread {
            socketInfo.socketConnected = isConnected
        } else {
            DispatchQueue.main.async { [socketInfo] in
                socketInfo.socketConnected = isConnected
            }
        }
    }

    func start() {
        if eventTap != nil {
            logger.warning("SwipeManager is already started")
            return
        }
        logger.info("SwipeManager start")
        eventTap = CGEvent.tapCreate(
            tap: .cghidEventTap,
            place: .headInsertEventTap,
            options: .listenOnly,
            eventsOfInterest: NSEvent.EventTypeMask.gesture.rawValue,
            callback: { proxy, type, cgEvent, me in
                let wrapper = Unmanaged<SwipeManager>.fromOpaque(me!)
                    .takeUnretainedValue()
                return wrapper.eventHandler(
                    proxy: proxy,
                    eventType: type,
                    cgEvent: cgEvent
                )
            },
            userInfo: Unmanaged.passUnretained(self).toOpaque()
        )
        if eventTap == nil {
            logger.error("SwipeManager couldn't create event tap")
            return
        }

        let runLoopSource = CFMachPortCreateRunLoopSource(nil, eventTap, 0)
        CFRunLoopAddSource(
            CFRunLoopGetCurrent(),
            runLoopSource,
            CFRunLoopMode.commonModes
        )
        CGEvent.tapEnable(tap: eventTap!, enable: true)

        connectSocket()
    }

    func stop() {
        logger.info("stop the app")
        if let eventTap {
            CGEvent.tapEnable(tap: eventTap, enable: false)
            CFMachPortInvalidate(eventTap)
            self.eventTap = nil
        }
        commandQueue.async { [weak self] in
            guard let self else { return }
            self.socket?.close()
            self.socket = nil
            self.setSocketConnected(false)
        }
    }

    private func eventHandler(
        proxy: CGEventTapProxy,
        eventType: CGEventType,
        cgEvent: CGEvent
    ) -> Unmanaged<CGEvent>? {
        if eventType.rawValue == NSEvent.EventType.gesture.rawValue,
            let nsEvent = NSEvent(cgEvent: cgEvent)
        {
            touchEventHandler(nsEvent)
        } else if eventType == .tapDisabledByUserInput
            || eventType == .tapDisabledByTimeout
        {
            logger.info("SwipeManager tap disabled \(eventType.rawValue)")
            CGEvent.tapEnable(tap: eventTap!, enable: true)
        }
        return Unmanaged.passUnretained(cgEvent)
    }

    private func touchEventHandler(_ nsEvent: NSEvent) {
        let touches = nsEvent.allTouches()

        // Sometimes there are empty touch events that we have to skip. There are no empty touch events if Mission Control or App Expose use 3-finger swipes though.
        if touches.isEmpty {
            return
        }
        let touchesCount =
            touches.allSatisfy({ $0.phase == .ended }) ? 0 : touches.count
        if touchesCount == 0 {
            stopGesture()
        } else {
            processTouches(touches: touches, count: touchesCount)
        }
    }

    private func stopGesture() {
        if state == .began {
            state = .ended
            handleGesture()
            clearEventState()
        }
    }

    private func processTouches(touches: Set<NSTouch>, count: Int) {
        if state != .began && count == gestureSettings.fingerCount {
            state = .began
        }
        if state == .began {
            accDisX += horizontalSwipeDistance(touches: touches)
        }
    }

    private func clearEventState() {
        accDisX = 0
        prevTouchPositions.removeAll()
    }

    private func handleGesture() {
        // filter
        if abs(accDisX) < gestureSettings.swipeThreshold {
            return
        }
        let direction: Direction =
            if gestureSettings.naturalSwipe {
                accDisX < 0 ? .next : .prev
            } else {
                accDisX < 0 ? .prev : .next
            }
        switchWorkspaceAsync(
            direction: direction,
            settings: currentWorkspaceSwitchSettings()
        )
    }

    private func currentWorkspaceSwitchSettings() -> WorkspaceSwitchSettings {
        workspaceSwitchSettings
    }

    private func horizontalSwipeDistance(touches: Set<NSTouch>) -> Float {
        var allRight = true
        var allLeft = true
        var sumDisX = Float(0)
        var sumDisY = Float(0)
        for touch in touches {
            let touchKey = touchIdentityKey(touch)
            let (disX, disY) = touchDistance(touch)
            allRight = allRight && disX >= 0
            allLeft = allLeft && disX <= 0
            sumDisX += disX
            sumDisY += disY

            if touch.phase == .ended {
                prevTouchPositions.removeValue(forKey: touchKey)
            } else {
                prevTouchPositions[touchKey] = touch.normalizedPosition
            }
        }
        // All fingers should move in the same direction.
        if !allRight && !allLeft {
            return 0
        }

        // Only horizontal swipes are interesting.
        if abs(sumDisX) <= abs(sumDisY) {
            return 0
        }

        return sumDisX
    }

    private func touchDistance(_ touch: NSTouch) -> (Float, Float) {
        guard let prevPosition = prevTouchPositions[touchIdentityKey(touch)]
        else {
            return (0, 0)
        }
        let position = touch.normalizedPosition
        return (
            Float(position.x - prevPosition.x),
            Float(position.y - prevPosition.y)
        )
    }

    private func touchIdentityKey(_ touch: NSTouch) -> ObjectIdentifier {
        ObjectIdentifier(touch.identity as AnyObject)
    }
}
