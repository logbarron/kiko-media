import Foundation

package enum CAProjectedSlotSelection {
    package enum Route: Sendable, Equatable {
        case local
        case remote(workerIndex: Int)
    }

    package struct PendingJob<Token: Hashable & Sendable>: Sendable {
        package let token: Token
        package let job: CAJob
        package let excludedRemoteSlot: CARemoteSlotKey?

        package init(
            token: Token,
            job: CAJob,
            excludedRemoteSlot: CARemoteSlotKey? = nil
        ) {
            self.token = token
            self.job = job
            self.excludedRemoteSlot = excludedRemoteSlot
        }
    }

    package struct DispatchPick<Token: Hashable & Sendable>: Sendable {
        package let token: Token
        package let route: Route
        package let slotIndex: Int
        package let slotID: String

        package init(
            token: Token,
            route: Route,
            slotIndex: Int,
            slotID: String
        ) {
            self.token = token
            self.route = route
            self.slotIndex = slotIndex
            self.slotID = slotID
        }
    }

    package struct HoldPick<Token: Hashable & Sendable>: Sendable {
        package let token: Token
        package let wakeAtMS: CAMilliseconds
        package let targetSlotID: String

        package init(
            token: Token,
            wakeAtMS: CAMilliseconds,
            targetSlotID: String
        ) {
            self.token = token
            self.wakeAtMS = wakeAtMS
            self.targetSlotID = targetSlotID
        }
    }

    package struct Plan<Token: Hashable & Sendable>: Sendable {
        package let dispatches: [DispatchPick<Token>]
        package let holds: [HoldPick<Token>]
        package let consumedTokens: [Token]
        package let clearedExcludedTokens: [Token]

        package init(
            dispatches: [DispatchPick<Token>],
            holds: [HoldPick<Token>],
            consumedTokens: [Token],
            clearedExcludedTokens: [Token]
        ) {
            self.dispatches = dispatches
            self.holds = holds
            self.consumedTokens = consumedTokens
            self.clearedExcludedTokens = clearedExcludedTokens
        }

        package var hasSelections: Bool {
            !dispatches.isEmpty || !holds.isEmpty
        }
    }

    package struct MachineContext: Sendable, Equatable {
        package let machine: CAMachine
        package let route: Route

        package init(machine: CAMachine, route: Route) {
            self.machine = machine
            self.route = route
        }

        package var slots: [CASlot] {
            machine.slots
        }

        package func remoteSlotKey(slotIndex: Int) -> CARemoteSlotKey? {
            guard case .remote(let workerIndex) = route else {
                return nil
            }
            return CARemoteSlotKey(workerIndex: workerIndex, slotIndex: slotIndex)
        }
    }

    package struct Assembly: Sendable {
        package let machineContexts: [MachineContext]
        private let remoteSlotRefsByKey: [CARemoteSlotKey: CASlotRef]

        package init(machineContexts: [MachineContext]) {
            self.machineContexts = machineContexts
            var remoteSlotRefsByKey: [CARemoteSlotKey: CASlotRef] = [:]
            remoteSlotRefsByKey.reserveCapacity(machineContexts.count)
            for (machineIndex, machineContext) in machineContexts.enumerated() {
                guard case .remote(let workerIndex) = machineContext.route else { continue }
                for slotIndex in machineContext.slots.indices {
                    remoteSlotRefsByKey[CARemoteSlotKey(workerIndex: workerIndex, slotIndex: slotIndex)] = CASlotRef(
                        machineIndex: machineIndex,
                        slotIndex: slotIndex
                    )
                }
            }
            self.remoteSlotRefsByKey = remoteSlotRefsByKey
        }

        package func plan<Token: Hashable & Sendable>(
            pendingJobs: [PendingJob<Token>],
            nowMS: CAMilliseconds,
            maxCount: Int
        ) -> Plan<Token> {
            guard maxCount > 0,
                  !pendingJobs.isEmpty,
                  !machineContexts.isEmpty else {
                return Plan(
                    dispatches: [],
                    holds: [],
                    consumedTokens: [],
                    clearedExcludedTokens: []
                )
            }

            var pendingByToken: [Token: PendingJob<Token>] = [:]
            pendingByToken.reserveCapacity(pendingJobs.count)
            let solverJobs = pendingJobs.map { pendingJob in
                pendingByToken[pendingJob.token] = pendingJob
                return CAPendingPickJob(
                    token: pendingJob.token,
                    job: pendingJob.job,
                    excludedSlot: pendingJob.excludedRemoteSlot.flatMap { remoteSlotRefsByKey[$0] }
                )
            }

            let pickResult = ComplexityAwareScheduler.pickTwoStageBatch(
                pendingJobs: solverJobs,
                machines: machineContexts.map(\.machine),
                nowMS: nowMS,
                maxReadyNowCount: maxCount
            )
            guard !pickResult.readyNowPicks.isEmpty || !pickResult.reservationPicks.isEmpty else {
                return Plan(
                    dispatches: [],
                    holds: [],
                    consumedTokens: [],
                    clearedExcludedTokens: []
                )
            }

            var dispatches: [DispatchPick<Token>] = []
            var holds: [HoldPick<Token>] = []
            var consumedTokens: [Token] = []
            var clearedExcludedTokens: [Token] = []
            dispatches.reserveCapacity(pickResult.readyNowPicks.count)
            holds.reserveCapacity(pickResult.reservationPicks.count)
            consumedTokens.reserveCapacity(pickResult.readyNowPicks.count + pickResult.reservationPicks.count)
            clearedExcludedTokens.reserveCapacity(pickResult.clearedExcludedTokens.count)

            for token in pickResult.clearedExcludedTokens {
                appendUnique(token, to: &clearedExcludedTokens)
            }

            for pick in pickResult.readyNowPicks {
                guard let pendingJob = pendingByToken[pick.token],
                      machineContexts.indices.contains(pick.slot.machineIndex) else {
                    continue
                }
                let machineContext = machineContexts[pick.slot.machineIndex]
                guard machineContext.slots.indices.contains(pick.slot.slotIndex) else {
                    continue
                }
                let targetSlot = machineContext.slots[pick.slot.slotIndex]
                consumedTokens.append(pick.token)

                if let excluded = pendingJob.excludedRemoteSlot,
                   let chosenRemoteSlot = machineContext.remoteSlotKey(slotIndex: pick.slot.slotIndex),
                   chosenRemoteSlot != excluded {
                    appendUnique(pick.token, to: &clearedExcludedTokens)
                }

                dispatches.append(
                    DispatchPick(
                        token: pick.token,
                        route: machineContext.route,
                        slotIndex: pick.slot.slotIndex,
                        slotID: targetSlot.id
                    )
                )
            }

            for pick in pickResult.reservationPicks {
                guard let pendingJob = pendingByToken[pick.token],
                      machineContexts.indices.contains(pick.slot.machineIndex) else {
                    continue
                }
                let machineContext = machineContexts[pick.slot.machineIndex]
                guard machineContext.slots.indices.contains(pick.slot.slotIndex) else {
                    continue
                }
                let targetSlot = machineContext.slots[pick.slot.slotIndex]
                consumedTokens.append(pick.token)

                if let excluded = pendingJob.excludedRemoteSlot,
                   let chosenRemoteSlot = machineContext.remoteSlotKey(slotIndex: pick.slot.slotIndex),
                   chosenRemoteSlot != excluded {
                    appendUnique(pick.token, to: &clearedExcludedTokens)
                }

                holds.append(
                    HoldPick(
                        token: pick.token,
                        wakeAtMS: max(pick.score.tReadySlotMS, nowMS),
                        targetSlotID: targetSlot.id
                    )
                )
            }

            return Plan(
                dispatches: dispatches,
                holds: holds,
                consumedTokens: consumedTokens,
                clearedExcludedTokens: clearedExcludedTokens
            )
        }
    }

    package static func assemble(
        nowMS: CAMilliseconds,
        localRemainingMS: [Double],
        topology: CATopologyModelBuildResult,
        remoteWorkers: [ThunderboltDispatcher.CAWorkerSnapshot] = []
    ) -> Assembly {
        var machineContexts: [MachineContext] = []
        machineContexts.reserveCapacity(topology.machineProfiles.count)

        guard let localMachineProfile = topology.machineProfiles.first else {
            return Assembly(machineContexts: [])
        }
        let localSlotCount = topology.slotBindings.reduce(into: 0) { partial, binding in
            if binding.machineIndex == 0 {
                partial += 1
            }
        }
        var localSlots: [CASlot] = []
        localSlots.reserveCapacity(localSlotCount)
        for localSlotIndex in 0..<localSlotCount {
            let readyAtMS = if localSlotIndex < localRemainingMS.count {
                nowMS + localRemainingMS[localSlotIndex]
            } else {
                nowMS
            }
            localSlots.append(
                CASlot(
                    id: "local#s\(localSlotIndex + 1)",
                    readyAtMS: readyAtMS
                )
            )
        }
        machineContexts.append(
            MachineContext(
                machine: CAMachine(
                    id: localMachineProfile.id,
                    slots: localSlots,
                    msPerFrameC1: localMachineProfile.msPerFrameC1,
                    fixedOverheadMS: localMachineProfile.fixedOverheadMS,
                    degradationCurve: localMachineProfile.degradationCurve,
                    txInMS: localMachineProfile.txInMS,
                    txOutMS: localMachineProfile.txOutMS,
                    publishOverheadMS: localMachineProfile.publishOverheadMS,
                    modeledConcurrencyCap: localMachineProfile.modeledConcurrencyCap
                ),
                route: .local
            )
        )

        guard topology.machineProfiles.count > 1 else {
            return Assembly(machineContexts: machineContexts)
        }

        let workersByHost = remoteWorkers.reduce(into: [String: ThunderboltDispatcher.CAWorkerSnapshot]()) { partial, worker in
            partial[worker.host] = worker
        }
        let hostsByMachineIndex = topology.machineIndexByHost.reduce(into: [Int: String]()) { partial, entry in
            partial[entry.value] = entry.key
        }

        for machineIndex in topology.machineProfiles.indices.dropFirst() {
            guard let host = hostsByMachineIndex[machineIndex],
                  let worker = workersByHost[host] else {
                continue
            }
            let profile = topology.machineProfiles[machineIndex]

            let slots = worker.slots.map { slot in
                let readyAtMS = if slot.isBusy {
                    nowMS + max(1, slot.estimatedRemainingMS ?? 1_000)
                } else {
                    nowMS
                }
                return CASlot(
                    id: slot.id,
                    readyAtMS: readyAtMS,
                    isDown: slot.isDown
                )
            }

            machineContexts.append(
                MachineContext(
                    machine: CAMachine(
                        id: profile.id,
                        slots: slots,
                        msPerFrameC1: profile.msPerFrameC1,
                        fixedOverheadMS: profile.fixedOverheadMS,
                        degradationCurve: profile.degradationCurve,
                        txInMS: profile.txInMS,
                        txOutMS: profile.txOutMS,
                        publishOverheadMS: profile.publishOverheadMS,
                        modeledConcurrencyCap: profile.modeledConcurrencyCap
                    ),
                    route: .remote(workerIndex: worker.workerIndex)
                )
            )
        }

        return Assembly(machineContexts: machineContexts)
    }

    private static func appendUnique<Token: Equatable>(
        _ token: Token,
        to tokens: inout [Token]
    ) {
        guard !tokens.contains(token) else { return }
        tokens.append(token)
    }
}

package struct CARemoteSlotKey: Hashable, Sendable, Equatable {
    package let workerIndex: Int
    package let slotIndex: Int

    package init(workerIndex: Int, slotIndex: Int) {
        self.workerIndex = workerIndex
        self.slotIndex = slotIndex
    }
}
