import Testing
@testable import KikoMediaCore

@Suite("Thunderbolt dispatcher source routing")
struct ThunderboltDispatcherRoutingTests {
    @Test("hostname worker resolves to matching bridge source")
    func hostnameWorkerResolvesToMatchingBridgeSource() {
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )

        let sourceIP = ThunderboltDispatcher.sourceIPForWorkerHost(
            "localhost",
            bridgeSources: [bridge]
        )
        #expect(sourceIP == "127.0.0.1")
    }

    @Test("unresolvable host has no source route")
    func unresolvableWorkerHostHasNoSourceRoute() {
        let bridge = ThunderboltDispatcher.BridgeSource(
            name: "bridge-test",
            ip: "127.0.0.1",
            network: 0x7F00_0000,
            mask: 0xFF00_0000
        )

        let sourceIP = ThunderboltDispatcher.sourceIPForWorkerHost(
            "not a valid host",
            bridgeSources: [bridge]
        )
        #expect(sourceIP == nil)
    }
}
