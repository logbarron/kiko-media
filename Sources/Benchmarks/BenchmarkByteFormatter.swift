import Foundation

enum BenchmarkByteFormatter {
    static func format(_ bytes: Int) -> String {
        format(Int64(bytes))
    }

    static func format(_ bytes: Int64) -> String {
        let value = max(Int64(0), bytes)

        if value < 1_000_000_000 {
            let mb = Double(value) / 1_000_000
            if mb >= 100 {
                return String(format: "%.0f MB", mb)
            }
            if mb >= 1 {
                return String(format: "%.1f MB", mb)
            }
            return String(format: "%.0f KB", Double(value) / 1_000)
        }

        let gb = Double(value) / 1_000_000_000
        if gb >= 1000 {
            return String(format: "%.1f TB", gb / 1000)
        }
        return String(format: "%.0f GB", gb)
    }
}
