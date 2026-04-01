import KikoMediaApp

@main
struct KikoMedia {
    static func main() async throws {
        try await KikoMediaAppRuntime.run()
    }
}
