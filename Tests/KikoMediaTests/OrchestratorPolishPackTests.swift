import Foundation
import Testing
@testable import Orchestrator

@Suite("Orchestrator polish pack")
struct OrchestratorPolishPackTests {
    @Test("status layout constants stay centralized")
    func statusLayoutConstantsCentralized() {
        #expect(StatusDashboardLayout.innerWidth == StatusDashboardLayout.boxWidth - 6)
        #expect(StatusDashboardLayout.sectionDividerWidth == StatusDashboardLayout.boxWidth - 1)
    }

    @Test("gallery header and rows share column alignment")
    func statusGalleryHeaderAlignment() {
        let header = statusDashboardGalleryHeaderRow()
        let row = statusDashboardGalleryRow(label: "Images", queued: 7, processing: 42, total: 123)

        #expect(header.count == row.count)
        let headerColumns = extractGalleryColumns(from: header)
        let rowColumns = extractGalleryColumns(from: row)
        #expect(headerColumns.count == 4)
        #expect(rowColumns.count == 4)
        #expect(headerColumns.map(\.count) == rowColumns.map(\.count))
        #expect(headerColumns[1].trimmingCharacters(in: .whitespaces) == "queued")
        #expect(headerColumns[2].trimmingCharacters(in: .whitespaces) == "processing")
        #expect(headerColumns[3].trimmingCharacters(in: .whitespaces) == "total")
        #expect(rowColumns[1].trimmingCharacters(in: .whitespaces) == "7")
        #expect(rowColumns[2].trimmingCharacters(in: .whitespaces) == "42")
        #expect(rowColumns[3].trimmingCharacters(in: .whitespaces) == "123")
    }

    @Test("timezone quick picks include non-US regions")
    func timezonePickerSupportsGlobalQuickPicks() {
        let identifiers = Set(timezoneQuickPickOptions.map(\.identifier))
        #expect(identifiers.contains("America/New_York"))
        #expect(identifiers.contains("Europe/London"))
        #expect(identifiers.contains("Asia/Tokyo"))
    }

    @Test("moderation URL parser reads localhost port from Caddy block")
    func moderationURLParserReadsLocalhostPort() {
        let caddy = """
        localhost:9090, 127.0.0.1:9090 {
            bind 127.0.0.1
        }
        """
        #expect(moderationUIURL(from: caddy) == "http://localhost:9090")
    }

    @Test("moderation URL parser returns nil when localhost binding is absent")
    func moderationURLParserReturnsNilWithoutLocalhostBinding() {
        let caddy = "https://example.com:8443 {\n}\n"
        #expect(moderationUIURL(from: caddy) == nil)
    }

    @Test("repo-root discovery message includes attempted command context")
    func repoRootDiscoveryErrorMentionsCommand() {
        #expect(repoRootDiscoveryErrorMessage(attemptedCommand: nil) == "Cannot find kiko-media repo root.")
        let contextual = repoRootDiscoveryErrorMessage(attemptedCommand: "orchestrator --status")
        #expect(contextual.contains("--status"))
        #expect(contextual.contains("command:"))
    }

    @Test("thunderbolt worker labels are one-indexed")
    func thunderboltWorkerLabelsOneIndexed() {
        #expect(thunderboltWorkerLabel(index: 0) == "W1")
        #expect(thunderboltWorkerLabel(index: 1) == "W2")
        #expect(thunderboltWorkerLabel(index: 2) == "W3")
    }

    private func extractGalleryColumns(from line: String) -> [String] {
        let chars = Array(line)
        let labelStart = 2
        let queuedStart = labelStart + StatusDashboardLayout.galleryLabelWidth + 3
        let processingStart = queuedStart + StatusDashboardLayout.galleryQueuedWidth + 3
        let totalStart = processingStart + StatusDashboardLayout.galleryProcessingWidth + 3

        let label = String(chars[labelStart..<queuedStart - 3])
        let queued = String(chars[queuedStart..<processingStart - 3])
        let processing = String(chars[processingStart..<totalStart - 3])
        let total = String(chars[totalStart..<min(totalStart + StatusDashboardLayout.galleryTotalWidth, chars.count)])
        return [label, queued, processing, total]
    }
}
