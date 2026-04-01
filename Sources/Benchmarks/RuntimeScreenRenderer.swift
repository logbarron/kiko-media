import Foundation

struct BenchmarkRuntimeField {
    let label: String
    let value: String
}

enum BenchmarkRuntimeSemantic {
    case success
    case warning
    case error
}

struct BenchmarkRuntimeInlineField {
    let label: String
    let value: String
    let semantic: BenchmarkRuntimeSemantic?

    init(label: String, value: String, semantic: BenchmarkRuntimeSemantic? = nil) {
        self.label = label
        self.value = value
        self.semantic = semantic
    }
}

enum BenchmarkRuntimeTableAlignment {
    case left
    case right
}

struct BenchmarkRuntimeTableColumn {
    let header: String
    let width: Int
    let alignment: BenchmarkRuntimeTableAlignment

    init(
        header: String,
        width: Int,
        alignment: BenchmarkRuntimeTableAlignment = .left
    ) {
        self.header = header
        self.width = width
        self.alignment = alignment
    }
}

struct BenchmarkRuntimeMenuRow {
    let title: String
    let details: [String]
}

enum BenchmarkRuntimeRenderer {
    static func printSectionTitle(_ title: String) {
        BenchOutput.line("")
        BenchOutput.line("  \(ConsoleUI.bold)\(title)\(ConsoleUI.reset)")
        BenchOutput.line("")
    }

    static func printSubsectionTitle(_ title: String, includeTrailingBlankLine: Bool = true) {
        BenchOutput.line("  \(ConsoleUI.bold)\(title)\(ConsoleUI.reset)")
        if includeTrailingBlankLine {
            BenchOutput.line("")
        }
    }

    static func printBody(_ text: String) {
        BenchOutput.line("  \(text)")
    }

    static func printField(
        _ label: String,
        _ value: String,
        semantic: BenchmarkRuntimeSemantic? = nil
    ) {
        BenchOutput.line("  \(ConsoleUI.dim)\(label):\(ConsoleUI.reset) \(semanticValue(value, semantic: semantic))")
    }

    static func printDetail(_ text: String) {
        BenchOutput.line("    \(ConsoleUI.dim)\(text)\(ConsoleUI.reset)")
    }

    static func printMetricItem(_ label: String, _ detail: String) {
        BenchOutput.line("  \(ConsoleUI.bold)\(label)\(ConsoleUI.reset)")
        BenchOutput.line("    \(detail)")
    }

    static func printStatsRow(_ label: String, _ detail: String, _ stats: Stats) {
        let pad = String(repeating: " ", count: max(1, 36 - label.count - detail.count))
        BenchOutput.line("  \(label) \(detail)\(pad)\(stats.summary)")
    }

    static func printValueRow(_ label: String, _ value: String) {
        let pad = String(repeating: " ", count: max(1, 40 - label.count))
        BenchOutput.line("  \(label)\(pad)\(value)")
    }

    static func printInlineFieldLine(
        _ fields: [BenchmarkRuntimeInlineField],
        indent: String = "    "
    ) {
        let rendered = fields.map { field in
            "\(ConsoleUI.dim)\(field.label):\(ConsoleUI.reset) \(semanticValue(field.value, semantic: field.semantic))"
        }
        BenchOutput.line("\(indent)\(rendered.joined(separator: "  "))")
    }

    static func printTableHeader(
        _ columns: [BenchmarkRuntimeTableColumn],
        indent: String = "  ",
        columnSeparator: String = "  "
    ) {
        let header = columns.map { column in
            padded(column.header, width: column.width, alignment: column.alignment)
        }.joined(separator: columnSeparator)
        BenchOutput.line("\(indent)\(ConsoleUI.dim)\(header)\(ConsoleUI.reset)")
        BenchOutput.line("\(indent)\(ConsoleUI.dim)\(String(repeating: "-", count: header.count))\(ConsoleUI.reset)")
    }

    static func printTableRow(
        _ values: [String],
        columns: [BenchmarkRuntimeTableColumn],
        semantics: [BenchmarkRuntimeSemantic?] = [],
        indent: String = "  ",
        columnSeparator: String = "  "
    ) {
        var rendered: [String] = []
        rendered.reserveCapacity(columns.count)

        for index in columns.indices {
            let column = columns[index]
            let value = index < values.count ? values[index] : ""
            let semantic = index < semantics.count ? semantics[index] : nil
            let paddedValue = padded(value, width: column.width, alignment: column.alignment)
            rendered.append(semanticValue(paddedValue, semantic: semantic))
        }

        BenchOutput.line("\(indent)\(rendered.joined(separator: columnSeparator))")
    }

    static func printMenuRow(_ number: Int, title: String, details: [String]) {
        BenchOutput.line("\(ConsoleUI.listItemIndent)\(ConsoleUI.bold)\(number).\(ConsoleUI.reset) \(title)")
        for detail in details {
            BenchOutput.line("\(ConsoleUI.listDetailIndent)\(ConsoleUI.dim)\(detail)\(ConsoleUI.reset)")
        }
    }

    static func printFieldSection(_ title: String, fields: [BenchmarkRuntimeField]) {
        printSectionTitle(title)
        for field in fields {
            printField(field.label, field.value)
        }
    }

    static func printMenuSection(
        _ title: String,
        rows: [BenchmarkRuntimeMenuRow],
        startAt: Int = 1
    ) {
        printSectionTitle(title)
        for (offset, row) in rows.enumerated() {
            printMenuRow(startAt + offset, title: row.title, details: row.details)
        }
    }

    private static func semanticValue(_ value: String, semantic: BenchmarkRuntimeSemantic?) -> String {
        guard let semantic else { return value }
        return "\(semanticColor(semantic))\(value)\(ConsoleUI.reset)"
    }

    private static func semanticColor(_ semantic: BenchmarkRuntimeSemantic) -> String {
        switch semantic {
        case .success:
            return ConsoleUI.green
        case .warning:
            return ConsoleUI.yellow
        case .error:
            return ConsoleUI.red
        }
    }

    private static func padded(
        _ value: String,
        width: Int,
        alignment: BenchmarkRuntimeTableAlignment
    ) -> String {
        let padCount = max(0, width - value.count)
        let pad = String(repeating: " ", count: padCount)
        switch alignment {
        case .left:
            return value + pad
        case .right:
            return pad + value
        }
    }
}
