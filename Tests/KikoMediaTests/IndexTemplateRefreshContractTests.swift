import Foundation
import Testing
import WebKit

@Suite("Index Template Refresh Contracts")
struct IndexTemplateRefreshContractTests {
    @Test("repeated prompt and control refresh requests coalesce into one trailing fetch")
    @MainActor
    func repeatedManualRefreshRequestsCoalesce() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        try await page.enqueueFetch(FetchStub.pending(id: "manual-refresh-1"))
        try await page.enqueueFetch(FetchStub.gallery(total: 2))
        try await page.run("""
            window.__codexTest.firstRefreshSettled = false;
            window.__codexTest.firstRefreshPromise = refreshGalleryNow('pull').finally(() => {
                window.__codexTest.firstRefreshSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.fetchLog.length === 2")

        try await page.run("""
            refreshGalleryNow('prompt');
            refreshGalleryNow('control');
        """)
        #expect(try await page.int("window.__codexTest.fetchLog.length") == 2)

        try await page.resolvePendingFetch(id: "manual-refresh-1", with: FetchStub.gallery(total: 1))
        try await page.waitUntil("""
            window.__codexTest.firstRefreshSettled === true &&
            window.__codexTest.fetchLog.length === 3 &&
            document.getElementById('grid').getAttribute('aria-busy') === 'false'
        """)

        #expect(try await page.int("window.__codexTest.fetchLog.length") == 3)
        #expect(try await page.string("document.getElementById('refreshStatus').getAttribute('data-state')") == "pull")
        #expect(try await page.bool("document.getElementById('refreshStatus').classList.contains('visible')") == false)
    }

    @Test("manual refresh queues a trailing refresh without overlapping fetches")
    @MainActor
    func manualRefreshQueuesTrailingRun() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        try await page.enqueueFetch(FetchStub.pending(id: "manual-refresh-1"))
        try await page.enqueueFetch(FetchStub.gallery(total: 2))
        try await page.run("""
            window.__codexTest.firstRefreshSettled = false;
            window.__codexTest.firstRefreshPromise = refreshGalleryNow('pull').finally(() => {
                window.__codexTest.firstRefreshSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.fetchLog.length === 2")

        try await page.tap("#newUploadsPrompt")
        #expect(try await page.int("window.__codexTest.fetchLog.length") == 2)

        try await page.resolvePendingFetch(id: "manual-refresh-1", with: FetchStub.gallery(total: 1))
        try await page.waitUntil("""
            window.__codexTest.fetchLog.length === 3 &&
            document.getElementById('grid').getAttribute('aria-busy') === 'false'
        """)

        #expect(try await page.int("window.__codexTest.fetchLog.length") == 3)
        #expect(try await page.string("document.getElementById('refreshStatus').getAttribute('data-state')") == "pull")
        #expect(try await page.bool("document.getElementById('refreshStatus').classList.contains('visible')") == false)
        #expect(try await page.string("document.getElementById('grid').getAttribute('aria-busy')") == "false")
    }

    @Test("stale gallery polls do not surface a prompt after a newer manual refresh wins")
    @MainActor
    func stalePollResponsesAreSuppressed() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 3)

        try await page.resetGalleryPollTimer()

        try await page.enqueueFetch(FetchStub.pending(id: "stale-poll"))
        try await page.run("""
            window.__codexTest.pollSettled = false;
            window.__codexTest.pollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.pollSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.fetchLog.length === 2")

        try await page.enqueueFetch(FetchStub.gallery(total: 5))
        try await page.run("""
            window.__codexTest.refreshSettled = false;
            window.__codexTest.refreshPromise = refreshGalleryNow('prompt').finally(() => {
                window.__codexTest.refreshSettled = true;
            });
        """)
        try await page.waitUntil("""
            window.__codexTest.refreshSettled === true &&
            window.__codexTest.fetchLog.length === 3
        """)

        let timerCountBeforePollResolution = try await page.int("window.__codexTest.timerLog.length")

        try await page.resolvePendingFetch(id: "stale-poll", with: FetchStub.gallery(total: 9))
        try await page.waitUntil("window.__codexTest.pollSettled === true")

        #expect(try await page.string("document.getElementById('newUploadsPrompt').style.display") != "block")
        #expect(try await page.int("window.__codexTest.timerLog.length") == timerCountBeforePollResolution)
    }

    @Test("newer heartRevision syncs loaded heart counts without a full gallery refresh")
    @MainActor
    func newerHeartRevisionSyncsLoadedCounts() async throws {
        let page = try await IndexTemplatePage.load(initialFetchQueue: [
            FetchStub.gallery(
                total: 2,
                assets: [
                    FetchStub.galleryAsset(id: "asset-1", heartCount: 1),
                    FetchStub.galleryAsset(id: "asset-2", heartCount: 0),
                ],
                heartRevision: 1
            ),
        ])

        try await page.run("openLightbox(0);")
        try await page.resetGalleryPollTimer()
        try await page.enqueueFetch(FetchStub.gallery(total: 2, heartRevision: 2))
        try await page.enqueueFetch(FetchStub.heartCounts([
            "asset-1": 5,
            "asset-2": 3,
        ]))
        try await page.run("""
            window.__codexTest.heartPollSettled = false;
            window.__codexTest.heartPollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.heartPollSettled = true;
            });
        """)
        try await page.waitUntil("""
            window.__codexTest.heartPollSettled === true &&
            window.__codexTest.fetchLog.length === 3
        """)

        let state = try await page.dictionary("""
            (() => ({
                pollUrl: window.__codexTest.fetchLog[1].url,
                syncUrl: window.__codexTest.fetchLog[2].url,
                syncMethod: window.__codexTest.fetchLog[2].method,
                syncIdsLength: JSON.parse(window.__codexTest.fetchLog[2].body).ids.length,
                asset1Count: serverAssets.find(a => a.id === 'asset-1').heartCount,
                asset2Count: serverAssets.find(a => a.id === 'asset-2').heartCount,
                badgeText: document.querySelector('.grid-item[data-id="asset-1"] .heart-badge').textContent,
                lightboxCount: document.getElementById('lightboxHeartCount').textContent,
                lastRevision: lastSyncedHeartRevision,
            }))()
        """)

        #expect((state["pollUrl"] as? String) == "/api/gallery?limit=1&offset=0")
        #expect((state["syncUrl"] as? String) == "/api/heart-counts")
        #expect((state["syncMethod"] as? String) == "POST")
        #expect((state["syncIdsLength"] as? Int) == 2)
        #expect((state["asset1Count"] as? Int) == 5)
        #expect((state["asset2Count"] as? Int) == 3)
        #expect((state["badgeText"] as? String) == "♥ 5")
        #expect((state["lightboxCount"] as? String) == "5")
        #expect((state["lastRevision"] as? Int) == 2)
    }

    @Test("stale heart-count sync responses do not patch after a newer manual refresh wins")
    @MainActor
    func staleHeartSyncResponsesAreSuppressed() async throws {
        let page = try await IndexTemplatePage.load(initialFetchQueue: [
            FetchStub.gallery(
                total: 1,
                assets: [FetchStub.galleryAsset(id: "asset-1", heartCount: 1)],
                heartRevision: 1
            ),
        ])

        try await page.resetGalleryPollTimer()
        try await page.enqueueFetch(FetchStub.gallery(total: 1, heartRevision: 2))
        try await page.enqueueFetch(FetchStub.pending(id: "stale-heart-sync"))
        try await page.run("""
            window.__codexTest.staleHeartPollSettled = false;
            window.__codexTest.staleHeartPollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.staleHeartPollSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.fetchLog.length === 3")

        try await page.enqueueFetch(FetchStub.gallery(
            total: 1,
            assets: [FetchStub.galleryAsset(id: "asset-1", heartCount: 11)],
            heartRevision: 2
        ))
        try await page.run("""
            window.__codexTest.staleHeartRefreshSettled = false;
            window.__codexTest.staleHeartRefreshPromise = refreshGalleryNow('prompt').finally(() => {
                window.__codexTest.staleHeartRefreshSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.staleHeartRefreshSettled === true")

        try await page.resolvePendingFetch(
            id: "stale-heart-sync",
            with: FetchStub.heartCounts(["asset-1": 5])
        )
        try await page.waitUntil("window.__codexTest.staleHeartPollSettled === true")

        let state = try await page.dictionary("""
            (() => ({
                fetchCount: window.__codexTest.fetchLog.length,
                heartCount: serverAssets.find(a => a.id === 'asset-1').heartCount,
                lastRevision: lastSyncedHeartRevision,
            }))()
        """)

        #expect((state["fetchCount"] as? Int) == 4)
        #expect((state["heartCount"] as? Int) == 11)
        #expect((state["lastRevision"] as? Int) == 2)
    }

    @Test("append fetch stores heartRevision and prevents a false follow-up heart sync")
    @MainActor
    func appendFetchStoresHeartRevision() async throws {
        let page = try await IndexTemplatePage.load(initialFetchQueue: [
            FetchStub.gallery(
                total: 2,
                assets: [FetchStub.galleryAsset(id: "asset-1")],
                heartRevision: 4
            ),
        ])

        try await page.enqueueFetch(FetchStub.gallery(
            total: 2,
            assets: [FetchStub.galleryAsset(id: "asset-2")],
            heartRevision: 4
        ))
        try await page.run("""
            window.__codexTest.appendSettled = false;
            window.__codexTest.appendPromise = fetchAssets(true).finally(() => {
                window.__codexTest.appendSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.appendSettled === true && window.__codexTest.fetchLog.length === 2")

        try await page.resetGalleryPollTimer()
        try await page.enqueueFetch(FetchStub.gallery(total: 2, heartRevision: 4))
        try await page.run("""
            window.__codexTest.appendPollSettled = false;
            window.__codexTest.appendPollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.appendPollSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.appendPollSettled === true")

        let state = try await page.dictionary("""
            (() => ({
                fetchCount: window.__codexTest.fetchLog.length,
                assetCount: serverAssets.length,
                lastRevision: lastSyncedHeartRevision,
            }))()
        """)

        #expect((state["fetchCount"] as? Int) == 3)
        #expect((state["assetCount"] as? Int) == 2)
        #expect((state["lastRevision"] as? Int) == 4)
    }

    @Test("heart sync chunks loaded IDs at 500 per request")
    @MainActor
    func heartSyncChunksLoadedIds() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        try await page.run("""
            serverAssets = Array.from({ length: 501 }, (_, index) => ({
                id: `asset-${index}`,
                type: 'image',
                heartCount: 0,
            }));
            serverTotal = 501;
            refreshBaselineTotal = 501;
            lastSyncedHeartRevision = 1;
        """)

        try await page.resetGalleryPollTimer()
        try await page.enqueueFetch(FetchStub.gallery(total: 501, heartRevision: 2))
        try await page.enqueueFetch(FetchStub.heartCounts([:]))
        try await page.enqueueFetch(FetchStub.heartCounts([:]))
        try await page.run("""
            window.__codexTest.chunkPollSettled = false;
            window.__codexTest.chunkPollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.chunkPollSettled = true;
            });
        """)
        try await page.waitUntil("""
            window.__codexTest.chunkPollSettled === true &&
            window.__codexTest.fetchLog.length === 4
        """)

        let state = try await page.dictionary("""
            (() => ({
                firstChunk: JSON.parse(window.__codexTest.fetchLog[2].body).ids.length,
                secondChunk: JSON.parse(window.__codexTest.fetchLog[3].body).ids.length,
                lastRevision: lastSyncedHeartRevision,
            }))()
        """)

        #expect((state["firstChunk"] as? Int) == 500)
        #expect((state["secondChunk"] as? Int) == 1)
        #expect((state["lastRevision"] as? Int) == 2)
    }

    @Test("apple touch browsers use touch listeners for pull tracking even when pointer events exist")
    @MainActor
    func appleTouchBrowsersUseTouchListeners() async throws {
        let page = try await IndexTemplatePage.load(
            initialGalleryTotal: 0,
            environmentSetupScript: """
            Object.defineProperty(window.navigator, 'vendor', {
                configurable: true,
                get: () => 'Apple Computer, Inc.'
            });
            Object.defineProperty(window.navigator, 'maxTouchPoints', {
                configurable: true,
                get: () => 5
            });
            window.PointerEvent = function PointerEvent() {};
            """
        )

        #expect(try await page.bool("shouldUsePointerPullGesturePath()") == false)
        #expect(try await page.bool("""
            window.__codexTest.listenerLog.some(entry =>
                entry.targetId === 'gridContainer' && entry.type === 'touchstart'
            )
        """))
        #expect(try await page.bool("""
            window.__codexTest.listenerLog.some(entry =>
                entry.targetId === 'gridContainer' && entry.type === 'pointerdown'
            )
        """) == false)
    }

    @Test("non-apple touch browsers also use touch listeners when touch input exists")
    @MainActor
    func nonAppleTouchBrowsersUseTouchListeners() async throws {
        let page = try await IndexTemplatePage.load(
            initialGalleryTotal: 0,
            environmentSetupScript: """
            Object.defineProperty(window.navigator, 'vendor', {
                configurable: true,
                get: () => 'Google Inc.'
            });
            Object.defineProperty(window.navigator, 'maxTouchPoints', {
                configurable: true,
                get: () => 5
            });
            window.PointerEvent = function PointerEvent() {};
            """
        )

        #expect(try await page.bool("shouldUsePointerPullGesturePath()") == false)
        #expect(try await page.bool("""
            window.__codexTest.listenerLog.some(entry =>
                entry.targetId === 'gridContainer' && entry.type === 'touchstart'
            )
        """))
        #expect(try await page.bool("""
            window.__codexTest.listenerLog.some(entry =>
                entry.targetId === 'gridContainer' && entry.type === 'pointerdown'
            )
        """) == false)
    }

    @Test("auth pause blocks gallery polling until resume re-arms it")
    @MainActor
    func authPauseAndResumeControlsGalleryPolling() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        try await page.resetGalleryPollTimer()
        try await page.enqueueFetch(FetchStub.status(401))
        try await page.run("""
            window.__codexTest.authPollSettled = false;
            window.__codexTest.authPollPromise = doGalleryPoll().finally(() => {
                window.__codexTest.authPollSettled = true;
            });
        """)
        try await page.waitUntil("window.__codexTest.authPollSettled === true")

        #expect(try await page.string("document.getElementById('verifyPrompt').style.display") == "block")

        try await page.run("startGalleryPoll();")
        #expect(try await page.int("window.__codexTest.pendingTimerCount()") == 0)

        try await page.run("resumeAllPolls();")
        #expect(try await page.string("document.getElementById('verifyPrompt').style.display") == "none")
        #expect(try await page.int("window.__codexTest.pendingTimerCount()") == 1)
    }

    @Test("first visit verification keeps the modal open in verified state")
    @MainActor
    func firstVisitVerificationKeepsModalOpen() async throws {
        let page = try await IndexTemplatePage.load(
            initialFetchQueue: [FetchStub.status(401)],
            waitForInitialFetchSettlement: false
        )
        try await page.enqueueFetch(FetchStub.status(204))
        try await page.enqueueFetch(FetchStub.gallery(total: 0))

        try await page.waitUntil("""
            window.__codexTest.fetchLog.length === 1 &&
            document.getElementById('modalOverlay').classList.contains('active') &&
            document.querySelector('.modal').getAttribute('data-state') === 'verifying'
        """)

        try await page.run("onTurnstileSuccess('test-token');")
        try await page.waitUntil("""
            window.__codexTest.fetchLog.length === 3 &&
            document.getElementById('loadingOverlay').style.display === 'none' &&
            document.getElementById('modalOverlay').classList.contains('active') &&
            document.querySelector('.modal').getAttribute('data-state') === 'verified'
        """)

        #expect(try await page.string("window.__codexTest.fetchLog[1].url") == "/api/turnstile/verify")
        #expect(try await page.string("window.__codexTest.fetchLog[2].url") == "/api/gallery?limit=24&offset=0")
    }

    @Test("mid-session reauth still auto-dismisses the modal after success")
    @MainActor
    func midSessionReauthStillAutoDismisses() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)
        try await page.enqueueFetch(FetchStub.status(204))

        try await page.run("""
            window.__codexTest.reauthResolved = false;
            window.__codexTest.reauthPromise = rerunVerification().then(() => {
                window.__codexTest.reauthResolved = true;
            });
        """)
        try await page.waitUntil("""
            document.getElementById('modalOverlay').classList.contains('active') &&
            document.querySelector('.modal').getAttribute('data-state') === 'verifying'
        """)

        try await page.run("onTurnstileSuccess('test-token');")
        try await page.waitUntil("""
            window.__codexTest.reauthResolved === true &&
            !document.getElementById('modalOverlay').classList.contains('active')
        """)

        #expect(try await page.string("window.__codexTest.fetchLog[1].url") == "/api/turnstile/verify")
    }

    @Test("pull reset clears the visual offset and hides refresh status")
    @MainActor
    func pullResetRestoresIdleVisualState() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        let state = try await page.dictionary("""
            (() => {
                startPullTracking(1, 0, 0, true, document.getElementById('grid'));
                handlePullMove(0, 160, 1);

                const grid = document.getElementById('grid');
                const refreshStatus = document.getElementById('refreshStatus');
                const before = {
                    transform: grid.style.transform,
                    visible: refreshStatus.classList.contains('visible'),
                    state: refreshStatus.getAttribute('data-state'),
                };

                resetPullGesture();

                return {
                    beforeTransform: before.transform,
                    beforeVisible: before.visible,
                    beforeState: before.state,
                    afterTransform: grid.style.transform,
                    afterVisible: refreshStatus.classList.contains('visible'),
                    afterState: refreshStatus.getAttribute('data-state'),
                    snapClassApplied: grid.classList.contains('pull-snap'),
                };
            })()
        """)

        #expect((state["beforeTransform"] as? String) != "translateY(0px)")
        #expect((state["beforeVisible"] as? Bool) == true)
        #expect((state["beforeState"] as? String) == "ready")
        #expect((state["afterTransform"] as? String) == "translateY(0px)")
        #expect((state["afterVisible"] as? Bool) == false)
        #expect((state["afterState"] as? String) == "pull")
        #expect((state["snapClassApplied"] as? Bool) == true)
    }

    @Test("viewport allows native zoom instead of forcing a zoom lock")
    @MainActor
    func viewportDoesNotDisableZoom() async throws {
        let page = try await IndexTemplatePage.load(initialGalleryTotal: 0)

        #expect(
            try await page.string("document.querySelector('meta[name=\"viewport\"]').getAttribute('content')")
            == "width=device-width, initial-scale=1.0"
        )
    }

    @Test("lightbox ignores multi-touch starts for swipe navigation")
    @MainActor
    func lightboxMultiTouchDoesNotSwipe() async throws {
        let page = try await IndexTemplatePage.load(initialFetchQueue: [[
            "status": 200,
            "json": [
                "assets": [
                    ["id": "asset-1", "type": "image", "heartCount": 0],
                    ["id": "asset-2", "type": "image", "heartCount": 0],
                ],
                "total": 2,
            ],
        ]])

        let state = try await page.dictionary("""
            (() => {
                openLightbox(0);

                const content = document.getElementById('lightboxContent');
                const touchStart = new Event('touchstart', { bubbles: true, cancelable: true });
                Object.defineProperty(touchStart, 'touches', {
                    configurable: true,
                    value: [{ screenX: 200 }, { screenX: 260 }]
                });
                Object.defineProperty(touchStart, 'changedTouches', {
                    configurable: true,
                    value: [{ screenX: 200 }, { screenX: 260 }]
                });
                content.dispatchEvent(touchStart);

                const touchEnd = new Event('touchend', { bubbles: true, cancelable: true });
                Object.defineProperty(touchEnd, 'changedTouches', {
                    configurable: true,
                    value: [{ screenX: 40 }]
                });
                content.dispatchEvent(touchEnd);

                return {
                    index: currentLightboxIndex,
                    swipeTracking: lightboxSwipeTracking,
                    active: document.getElementById('lightbox').classList.contains('active'),
                };
            })()
        """)

        #expect((state["index"] as? Int) == 0)
        #expect((state["swipeTracking"] as? Bool) == false)
        #expect((state["active"] as? Bool) == true)
    }
}

@MainActor
private final class IndexTemplatePage: NSObject, WKNavigationDelegate {
    private let webView: WKWebView
    private var loadContinuation: CheckedContinuation<Void, Error>?

    private init(initialFetchQueueJSON: String, environmentSetupScript: String) {
        let config = WKWebViewConfiguration()
        let controller = WKUserContentController()
        controller.addUserScript(
            WKUserScript(
                source: Self.bootstrapScript(
                    initialFetchQueueJSON: initialFetchQueueJSON,
                    environmentSetupScript: environmentSetupScript
                ),
                injectionTime: .atDocumentStart,
                forMainFrameOnly: true
            )
        )
        config.userContentController = controller
        self.webView = WKWebView(frame: .zero, configuration: config)
        super.init()
        self.webView.navigationDelegate = self
    }

    static func load(
        initialGalleryTotal: Int,
        environmentSetupScript: String = ""
    ) async throws -> IndexTemplatePage {
        try await load(
            initialFetchQueue: [FetchStub.gallery(total: initialGalleryTotal)],
            environmentSetupScript: environmentSetupScript
        )
    }

    static func load(
        initialFetchQueue: [[String: Any]],
        waitForInitialFetchSettlement: Bool = true,
        environmentSetupScript: String = ""
    ) async throws -> IndexTemplatePage {
        let page = try IndexTemplatePage(
            initialFetchQueueJSON: jsonLiteral(initialFetchQueue),
            environmentSetupScript: environmentSetupScript
        )
        try await page.loadTemplate(waitForInitialFetchSettlement: waitForInitialFetchSettlement)
        return page
    }

    func run(_ script: String) async throws {
        let wrapped = """
        (() => {
            \(script)
            return true;
        })()
        """
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            webView.evaluateJavaScript(wrapped) { _, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                continuation.resume()
            }
        }
    }

    func tap(_ selector: String) async throws {
        let selectorLiteral = try Self.stringLiteral(selector)
        try await run("""
            const element = document.querySelector(\(selectorLiteral));
            if (!element) { throw new Error('Missing element for tap'); }
            element.click();
        """)
    }

    func enqueueFetch(_ stub: [String: Any]) async throws {
        try await run("window.__codexTest.fetchQueue.push(\(try Self.jsonLiteral(stub)));")
    }

    func resolvePendingFetch(id: String, with stub: [String: Any]) async throws {
        try await run("""
            window.__codexTest.resolveFetch(
                \(try Self.stringLiteral(id)),
                \(try Self.jsonLiteral(stub))
            );
        """)
    }

    func resetGalleryPollTimer() async throws {
        try await run("""
            window.__codexTest.clearTimers();
            galleryPollTimer = null;
            galleryPollDelay = GALLERY_POLL_BASE_MS;
        """)
    }

    func waitUntil(_ condition: String, timeoutNanoseconds: UInt64 = 2_000_000_000) async throws {
        let deadline = DispatchTime.now().uptimeNanoseconds + timeoutNanoseconds
        while true {
            if try await bool(condition) {
                return
            }
            if DispatchTime.now().uptimeNanoseconds >= deadline {
                throw IndexTemplatePageError.timeout(condition)
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    func bool(_ expression: String) async throws -> Bool {
        if let boolValue = try await jsonValue("Boolean(\(expression))") as? Bool {
            return boolValue
        }
        throw IndexTemplatePageError.typeMismatch(expression)
    }

    func int(_ expression: String) async throws -> Int {
        if let numberValue = try await jsonValue(expression) as? NSNumber {
            return numberValue.intValue
        }
        if let intValue = try await jsonValue(expression) as? Int {
            return intValue
        }
        throw IndexTemplatePageError.typeMismatch(expression)
    }

    func string(_ expression: String) async throws -> String {
        if let stringValue = try await jsonValue(expression) as? String {
            return stringValue
        }
        throw IndexTemplatePageError.typeMismatch(expression)
    }

    func dictionary(_ script: String) async throws -> [String: Any] {
        if let dict = try await jsonValue(script) as? [String: Any] {
            return dict
        }
        throw IndexTemplatePageError.typeMismatch("dictionary")
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        resolveLoad(.success(()))
    }

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        resolveLoad(.failure(error))
    }

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        resolveLoad(.failure(error))
    }

    private func loadTemplate(waitForInitialFetchSettlement: Bool) async throws {
        let html = try Self.preparedTemplate()
        try await withCheckedThrowingContinuation { continuation in
            loadContinuation = continuation
            webView.loadHTMLString(html, baseURL: URL(string: "https://gallery.example.test/"))
        }
        if waitForInitialFetchSettlement {
            try await waitUntil("""
                document.readyState === 'complete' &&
                window.__codexTest.fetchLog.length >= 1 &&
                document.getElementById('loadingOverlay').style.display === 'none'
            """)
        } else {
            try await waitUntil("""
                document.readyState === 'complete' &&
                window.__codexTest.fetchLog.length >= 1
            """)
        }
    }

    private func resolveLoad(_ result: Result<Void, Error>) {
        guard let continuation = loadContinuation else { return }
        loadContinuation = nil
        continuation.resume(with: result)
    }

    private func jsonValue(_ script: String) async throws -> Any {
        let wrapped = """
        (() => {
            const __codexValue = \(script);
            return JSON.stringify(__codexValue);
        })()
        """
        let json = try await evaluateJSON(wrapped)
        guard let data = json.data(using: .utf8) else {
            throw IndexTemplatePageError.jsonEncoding
        }
        return try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
    }

    private func evaluateJSON(_ script: String) async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            webView.evaluateJavaScript(script) { value, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                guard let value = value as? String else {
                    continuation.resume(throwing: IndexTemplatePageError.typeMismatch("json"))
                    return
                }
                continuation.resume(returning: value)
            }
        }
    }

    private static func preparedTemplate() throws -> String {
        let root = try TestRepositoryRoot.resolve(
            from: #filePath,
            sentinels: ["Package.swift"]
        )
        let path = root.appendingPathComponent("deploy/index.html.template")
        var template = try String(contentsOf: path, encoding: .utf8)

        let replacements: [String: String] = [
            "__UPLOAD_CHUNK_SIZE_BYTES__": "1048576",
            "__TUSD_MAX_SIZE__": "10485760",
            "__PARALLEL_UPLOADS__": "2",
            "__UPLOAD_RETRY_BASE_MS__": "100",
            "__UPLOAD_RETRY_MAX_MS__": "800",
            "__UPLOAD_RETRY_STEPS__": "3",
            "__DEFAULT_PAGE_SIZE__": "24",
            "__TURNSTILE_SITEKEY__": "test-sitekey",
            "__TURNSTILE_ACTION__": "test-action",
            "__TURNSTILE_CDATA__": "test-cdata",
            "__GATE_ENABLED__": "false",
            "__POLL_MAX_INFLIGHT__": "2",
            "__GALLERY_POLL_BASE_MS__": "1000",
            "__GALLERY_POLL_MAX_MS__": "4000",
            "__PHOTO_THUMB_POLL_BASE_MS__": "1000",
            "__PHOTO_THUMB_POLL_MAX_MS__": "4000",
            "__PHOTO_PREVIEW_POLL_BASE_MS__": "1000",
            "__PHOTO_PREVIEW_POLL_MAX_MS__": "4000",
            "__VIDEO_PREVIEW_EARLY_BASE_MS__": "1000",
            "__VIDEO_PREVIEW_EARLY_MAX_MS__": "4000",
            "__VIDEO_PREVIEW_LATE_MS__": "8000",
            "__VIDEO_PREVIEW_EARLY_WINDOW_MS__": "5000",
        ]

        for (placeholder, value) in replacements {
            template = template.replacingOccurrences(of: placeholder, with: value)
        }

        template = template.replacingOccurrences(of: "<script src=\"/tus.min.js\"></script>\n", with: "")
        template = template.replacingOccurrences(of: "<link rel=\"preconnect\" href=\"https://challenges.cloudflare.com\">\n", with: "")
        template = template.replacingOccurrences(
            of: "<script src=\"https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit\"></script>\n",
            with: ""
        )

        return template
    }

    private static func bootstrapScript(
        initialFetchQueueJSON: String,
        environmentSetupScript: String
    ) -> String {
        """
        window.__codexTest = {
          fetchQueue: \(initialFetchQueueJSON),
          fetchLog: [],
          pendingFetches: {},
          listenerLog: [],
          timers: new Map(),
          timerLog: [],
          nextTimerId: 1,
          makeResponse(spec = {}) {
            const status = spec.status ?? 200;
            const payload = spec.json ?? {};
            const headers = spec.headers ?? {};
            return {
              status,
              ok: spec.ok ?? (status >= 200 && status < 300),
              json: async () => payload,
              headers: {
                get(name) {
                  const lowerName = String(name).toLowerCase();
                  for (const [key, value] of Object.entries(headers)) {
                    if (String(key).toLowerCase() === lowerName) return value;
                  }
                  return null;
                }
              }
            };
          },
          resolveFetch(id, spec) {
            const pending = this.pendingFetches[id];
            if (!pending) throw new Error(`Missing pending fetch: ${id}`);
            delete this.pendingFetches[id];
            pending.resolve(this.makeResponse(spec));
          },
          rejectFetch(id, message) {
            const pending = this.pendingFetches[id];
            if (!pending) throw new Error(`Missing pending fetch: ${id}`);
            delete this.pendingFetches[id];
            pending.reject(new Error(message));
          },
          clearTimers() {
            this.timers.clear();
            this.timerLog = [];
          },
          pendingTimerCount() {
            return this.timers.size;
          }
        };

        const __codexOriginalAddEventListener = EventTarget.prototype.addEventListener;
        EventTarget.prototype.addEventListener = function(type, listener, options) {
          window.__codexTest.listenerLog.push({
            type: String(type),
            targetId: this && typeof this.id === 'string' ? this.id : ''
          });
          return __codexOriginalAddEventListener.call(this, type, listener, options);
        };

        \(environmentSetupScript)

        window.fetch = function(url, options = {}) {
          const spec = window.__codexTest.fetchQueue.length > 0
            ? window.__codexTest.fetchQueue.shift()
            : { status: 200, json: { assets: [], total: 0 } };
          window.__codexTest.fetchLog.push({
            url: String(url),
            method: options.method || 'GET',
            body: typeof options.body === 'string' ? options.body : null
          });
          if (spec.pendingId) {
            return new Promise((resolve, reject) => {
              window.__codexTest.pendingFetches[spec.pendingId] = { resolve, reject };
            });
          }
          if (spec.rejectMessage) {
            return Promise.reject(new Error(spec.rejectMessage));
          }
          return Promise.resolve(window.__codexTest.makeResponse(spec));
        };

        window.setTimeout = function(callback, delay) {
          const id = window.__codexTest.nextTimerId++;
          window.__codexTest.timers.set(id, { callback, delay: delay || 0 });
          window.__codexTest.timerLog.push({ id, delay: delay || 0 });
          return id;
        };

        window.clearTimeout = function(id) {
          window.__codexTest.timers.delete(id);
        };

        window.alert = function() {};
        window.matchMedia = window.matchMedia || function() {
          return { matches: false, addEventListener() {}, removeEventListener() {} };
        };
        window.turnstile = {
          ready(callback) { callback(); },
          render() { return 1; },
          reset() {}
        };
        window.tus = {
          Upload: function() {
            return {
              start() {},
              abort() {},
              findPreviousUploads() { return Promise.resolve([]); },
              resumeFromPreviousUpload() {}
            };
          }
        };
        Math.random = () => 0.5;
        """
    }

    private static func jsonLiteral(_ object: Any) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: object)
        guard let string = String(data: data, encoding: .utf8) else {
            throw IndexTemplatePageError.jsonEncoding
        }
        return string
    }

    private static func stringLiteral(_ string: String) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: [string])
        guard
            let encoded = String(data: data, encoding: .utf8),
            encoded.count >= 2
        else {
            throw IndexTemplatePageError.jsonEncoding
        }
        return String(encoded.dropFirst().dropLast())
    }
}

private enum FetchStub {
    static func gallery(
        total: Int,
        assets: [[String: Any]] = [],
        heartRevision: Int? = nil
    ) -> [String: Any] {
        var json: [String: Any] = [
            "assets": assets,
            "total": total,
        ]
        if let heartRevision {
            json["heartRevision"] = heartRevision
        }
        return [
            "status": 200,
            "json": json,
        ]
    }

    static func pending(id: String) -> [String: Any] {
        ["pendingId": id]
    }

    static func heartCounts(_ heartCounts: [String: Int]) -> [String: Any] {
        [
            "status": 200,
            "json": [
                "heartCounts": heartCounts,
            ],
        ]
    }

    static func galleryAsset(id: String, type: String = "image", heartCount: Int = 0) -> [String: Any] {
        [
            "id": id,
            "type": type,
            "heartCount": heartCount,
        ]
    }

    static func status(_ status: Int) -> [String: Any] {
        ["status": status]
    }
}

private enum IndexTemplatePageError: Error {
    case jsonEncoding
    case timeout(String)
    case typeMismatch(String)
}
