import Foundation
import VideoToolbox

func printVideoEncoders() {
    BenchmarkRuntimeRenderer.printSubsectionTitle("Hardware Video Encoders")
    var list: CFArray?
    let status = VTCopyVideoEncoderList(nil, &list)
    guard status == noErr, let encoders = list as? [Any] else {
        BenchmarkRuntimeRenderer.printField("Encoder query", "failed (status: \(status))", semantic: .error)
        return
    }

    let columns: [BenchmarkRuntimeTableColumn] = [
        BenchmarkRuntimeTableColumn(header: "Encoder", width: 34),
        BenchmarkRuntimeTableColumn(header: "Type", width: 4),
        BenchmarkRuntimeTableColumn(header: "ID", width: 34),
    ]
    BenchmarkRuntimeRenderer.printTableHeader(columns)

    for item in encoders {
        guard let encoder = item as? [String: Any] else { continue }
        let name = encoder[kVTVideoEncoderList_DisplayName as String] as? String ?? "Unknown"
        let id = encoder[kVTVideoEncoderList_EncoderID as String] as? String ?? ""
        let isHW = encoder[kVTVideoEncoderList_IsHardwareAccelerated as String] as? Bool ?? false
        BenchmarkRuntimeRenderer.printTableRow(
            [name, isHW ? "HW" : "SW", id],
            columns: columns
        )
    }
}
