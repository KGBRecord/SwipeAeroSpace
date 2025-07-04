import SwiftUI

struct SettingsView: View {
    @AppStorage("threshold") private static var swipeThreshold: Double = 0.3
    @AppStorage("wrap") private var wrapWorkspace: Bool = false
    @AppStorage("natrual") private var naturalSwipe: Bool = true
    @AppStorage("skip-empty") private var skipEmpty: Bool = false
    @AppStorage("qwerty-flow") private var qwertyFlow: Bool = false
    @AppStorage("fingers") private var fingers: String = "Three"

    @State private var numberFormatter: NumberFormatter = {
        var nf = NumberFormatter()
        nf.numberStyle = .decimal
        return nf
    }()

    let numbers = ["Three", "Four"]

    var swipeManager: SwipeManager
    @ObservedObject var socketInfo: SocketInfo

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            VStack(alignment: .leading) {
                HStack {
                    Text("Socket Status: ")
                    Image(systemName: "circle.fill").foregroundStyle(
                        socketInfo.socketConnected ? .green : .red
                    )
                }
                if !socketInfo.socketConnected {
                    Button("Try to connect socket") {
                        swipeManager.connectSocket(reconnect: true)
                    }
                }
            }
            Form {
                TextField(
                    "Swipe Threshold",
                    value: SettingsView.$swipeThreshold,
                    formatter: numberFormatter,
                    prompt: Text("0.3")
                ).textFieldStyle(RoundedBorderTextFieldStyle()).frame(
                    maxWidth: 200
                )
            }

            Picker("Number of Fingers:", selection: $fingers) {
                ForEach(numbers, id: \.self) {
                    Text($0)
                }
            }
            .pickerStyle(.segmented)
            .frame(maxWidth: 400)
            .padding(.vertical, 4)

            VStack(alignment: .leading) {
                Toggle("Wrap Workspace", isOn: $wrapWorkspace)
                Text("Enable to jump between first and last workspaces")
                    .foregroundStyle(.secondary)
            }.padding(.vertical, 4)

            VStack(alignment: .leading) {
                Toggle("Natural Swipe", isOn: $naturalSwipe)
                Text("Disable to use reversed swipe ").foregroundStyle(
                    .secondary
                )
            }.padding(.vertical, 4)

            VStack(alignment: .leading) {
                Toggle("Skip Empty Workspace", isOn: $skipEmpty)
                Text("Enable to skip empty workspaces").foregroundStyle(
                    .secondary
                )
            }.padding(.vertical, 4)
            VStack(alignment: .leading) {
                Toggle("QWERTY flow swipe", isOn: $qwertyFlow)
                Text("Enable swiping by QWERTY flow").foregroundStyle(
                    .secondary
                )
            }.padding(.vertical, 4)

            LaunchAtLogin.Toggle {
                Text("Launch At Login")
            }
        }
        .padding(.horizontal, 32)
        .padding(.vertical, 24)

    }
}

struct SettingsView_Previews: PreviewProvider {
    static var swipeManager = SwipeManager()
    static var previews: some View {
        SettingsView(
            swipeManager: swipeManager,
            socketInfo: swipeManager.socketInfo
        )
    }
}
