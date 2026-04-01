import Foundation

func effectiveAdvancedDefaultOverrides(domain: String, ssdPath: String, eventTimezone: String) -> [String: String] {
    [
        "TURNSTILE_EXPECTED_HOSTNAME": domain,
        "EXTERNAL_SSD_PATH": ssdPath,
        "EVENT_TIMEZONE": eventTimezone,
    ]
}

// MARK: - Advanced Configuration

let advancedSections: [(String, [String])] = [
    ("Operational", ["PUBLIC_PORT", "INTERNAL_PORT", "BIND_ADDRESS", "BASE_DIRECTORY", "TUSD_MAX_SIZE", "HEALTH_CHECK_INTERVAL",
                     "JSON_MAX_BODY_BYTES", "WEBHOOK_RETRY_AFTER_SECONDS"]),
    ("Processing", ["MAX_CONCURRENT_IMAGES", "MAX_CONCURRENT_VIDEOS", "VIDEO_SCHEDULER_POLICY", "MAX_CONCURRENT_REBUILD_PROBES", "MAX_PENDING_WEBHOOK_JOBS", "THUMBNAIL_SIZE", "THUMBNAIL_QUALITY",
                     "PREVIEW_SIZE", "PREVIEW_QUALITY", "VIDEO_THUMBNAIL_SIZE", "VIDEO_THUMBNAIL_TIME",
                     "VIDEO_THUMBNAIL_QUALITY", "VIDEO_TRANSCODE_TIMEOUT", "VIDEO_TRANSCODE_PRESET"]),
    ("Frontend", ["UPLOAD_CHUNK_SIZE_BYTES", "PARALLEL_UPLOADS", "UPLOAD_RETRY_BASE_MS", "UPLOAD_RETRY_MAX_MS",
                   "UPLOAD_RETRY_STEPS", "POLL_MAX_INFLIGHT", "GALLERY_POLL_BASE_MS", "GALLERY_POLL_MAX_MS",
                   "PHOTO_THUMB_POLL_BASE_MS", "PHOTO_THUMB_POLL_MAX_MS", "PHOTO_PREVIEW_POLL_BASE_MS",
                   "PHOTO_PREVIEW_POLL_MAX_MS", "VIDEO_PREVIEW_EARLY_BASE_MS", "VIDEO_PREVIEW_EARLY_MAX_MS",
                   "VIDEO_PREVIEW_LATE_MS", "VIDEO_PREVIEW_EARLY_WINDOW_MS"]),
    ("Security", ["MAX_IMAGE_PIXELS", "MAX_IMAGE_DIMENSION", "MAX_COMPRESSION_RATIO"]),
    ("Database", ["SQLITE_BUSY_TIMEOUT", "SQLITE_CACHE_SIZE", "DEFAULT_PAGE_SIZE", "MAX_PAGE_SIZE",
                   "MAX_PAGE_OFFSET", "SQL_BATCH_SIZE"]),
    ("Session", ["SESSION_COOKIE_TTL", "SESSION_COOKIE_NAME", "TURNSTILE_VERIFY_TIMEOUT", "TURNSTILE_MAX_RESPONSE", "TURNSTILE_MAX_INFLIGHT_VERIFICATIONS", "TURNSTILE_OVERLOAD_RETRY_AFTER_SECONDS", "TURNSTILE_EXPECTED_HOSTNAME", "TURNSTILE_EXPECTED_ACTION", "TURNSTILE_EXPECTED_CDATA"]),
    ("Caching", ["CACHE_CONTROL"]),
    ("Internal", ["QUEUE_COMPACTION_THRESHOLD", "SHA256_BUFFER_SIZE"]),
]

func runAdvancedConfig(
    _ answers: inout Answers,
    defaults: [String: DefaultSpec],
    effectiveDefaultOverrides: [String: String],
    completedSteps: [(String, String)] = []
) {
    for (sectionName, vars) in advancedSections {
        let entries: [(envVar: String, spec: DefaultSpec)] = vars.compactMap { envVar in
            defaults[envVar].map { (envVar: envVar, spec: $0) }
        }

        redraw(completedSteps, subtitle: "Advanced configuration.")
        print()
        print("  \(bold)Advanced: \(sectionName)\(reset)")
        print()

        var editing = true
        while editing {
            for (i, entry) in entries.enumerated() {
                let envVar = entry.envVar
                let spec = entry.spec
                let current = answers.advanced[envVar] ?? spec.defaultValue
                let effectiveDefault = effectiveDefaultOverrides[envVar] ?? spec.defaultValue
                let isDefault = current == effectiveDefault
                let status = isDefault ? "\(dim)[default]\(reset)" : "\(bold)[changed]\(reset)"
                let valueStyle = isDefault ? dim : bold
                print("    \(bold)\(i + 1)\(reset)  \(spec.label) \(status)  \(valueStyle)\(current)\(reset)")
                print("\(listDetailIndent)\(dim)\(spec.description)\(reset)")
            }
            print()
            printHint("Enter a number to change, or press Enter for next section.")
            print()

            let choice = prompt("Option")
            if choice.isEmpty {
                editing = false
                continue
            }

            guard let n = Int(choice), (1...entries.count).contains(n) else {
                printError("Enter a number from 1 to \(entries.count), or press Enter to continue.")
                print()
                continue
            }

            let envVar = entries[n - 1].envVar
            let spec = entries[n - 1].spec
            let current = answers.advanced[envVar] ?? spec.defaultValue
            let effectiveDefault = effectiveDefaultOverrides[envVar] ?? spec.defaultValue
            print()
            print("  \(bold)\(spec.label)\(reset)")
            printHint(spec.description)
            printHint("Current: \(current)  Default: \(effectiveDefault)")
            print()

            while true {
                let input = prompt("New value", default: current)
                let normalizedInput = normalizedAdvancedValue(input, for: envVar)
                if validateSpec(normalizedInput, envVar: envVar, spec: spec) {
                    answers.advanced[envVar] = normalizedInput
                    print()
                    break
                }
                printError("Invalid value. \(spec.description)")
            }
        }
    }
}

func countAdvancedChanges(
    advancedValues: [String: String],
    defaults: [String: DefaultSpec],
    effectiveDefaultOverrides: [String: String]
) -> Int {
    var count = 0
    for (_, vars) in advancedSections {
        for envVar in vars {
            guard let spec = defaults[envVar] else { continue }
            let effectiveDefault = effectiveDefaultOverrides[envVar] ?? spec.defaultValue
            let current = advancedValues[envVar] ?? spec.defaultValue
            if current != effectiveDefault {
                count += 1
            }
        }
    }
    return count
}
