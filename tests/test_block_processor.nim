# beacon_chain
# Copyright (c) 2018-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [], gcsafe.}
{.used.}

import
  chronos,
  ssz_serialization,
  unittest2,
  taskpools,
  kzg4844 as kzg,
  kzg4844/kzg_abi,
  ../beacon_chain/conf,
  ../beacon_chain/spec/[beaconstate, forks, helpers,
    peerdas_helpers, state_transition],
  ../beacon_chain/gossip_processing/block_processor,
  ../beacon_chain/consensus_object_pools/[
    blockchain_dag, block_quarantine, block_clearance, column_quarantine,
    consensus_manager, envelope_quarantine
  ],
  ../beacon_chain/el/el_manager,
  ./[testblockutil, testdbutil, testutil]

from std/sequtils import mapIt
from std/strutils import rsplit
from chronos/unittest2/asynctests import asyncTest
from ../beacon_chain/consensus_object_pools/attestation_pool import
  AttestationPool, init
from ../beacon_chain/spec/column_map import supernodeMap
from ../beacon_chain/spec/eth2_apis/dynamic_fee_recipients import
  DynamicFeeRecipientsStore, init
from ../beacon_chain/validators/action_tracker import ActionTracker
from ../beacon_chain/validators/keystore_management import KeymanagerHost

block:
  template sourceDir: string = currentSourcePath.rsplit(DirSep, 1)[0]
  doAssert kzg.loadTrustedSetup(
    sourceDir &
      "/../vendor/nim-kzg4844/kzg4844/csources/src/trusted_setup.txt", 7).isOk

# Helper to create valid KZG blobs
func createValidKzgBlob(): kzg.KzgBlob =
  # Create a blob with valid field elements
  # Each field element must be < BLS modulus (top byte must be <= 114)
  const MAX_TOP_BYTE = 114
  var blob: array[kzg_abi.BYTES_PER_BLOB, byte]
  for i in 0 ..< kzg_abi.BYTES_PER_BLOB:
    if i mod kzg_abi.BYTES_PER_FIELD_ELEMENT == 0:  # First byte of each field element
      blob[i] = MAX_TOP_BYTE  # Safe value
    else:
      blob[i] = byte(i mod 256)
  kzg.KzgBlob(bytes: blob)

proc pruneAtFinalization(dag: ChainDAGRef) =
  if dag.needStateCachesAndForkChoicePruning():
    dag.pruneStateCachesDAG()

proc copyFixture[T](value: T): T =
  try:
    SSZ.decode(SSZ.encode(value), T)
  except SerializationError:
    raiseAssert "generated fixture must round trip"

suite "Block processor" & preset():
  setup:
    let
      rng = HmacDrbgContext.new()
      cfg = block:
        var res = defaultRuntimeConfig
        res.ALTAIR_FORK_EPOCH = GENESIS_EPOCH
        res.BELLATRIX_FORK_EPOCH = GENESIS_EPOCH
        res.CAPELLA_FORK_EPOCH = Epoch(1)
        res.DENEB_FORK_EPOCH = Epoch(2)
        res.ELECTRA_FORK_EPOCH = Epoch(3)
        res.FULU_FORK_EPOCH = Epoch(4)
        res.GLOAS_FORK_EPOCH = Epoch(5)
        res
      db = cfg.makeTestDB(SLOTS_PER_EPOCH)
      validatorMonitor = newClone(ValidatorMonitor.init(cfg))
      dag = ChainDAGRef.init(cfg, db, validatorMonitor, {})
      taskpool = Taskpool.new()
      quarantine = newClone(Quarantine.init(cfg))
      dataColumnQuarantine = newClone(FuluColumnQuarantine())
      gloasColumnQuarantine = newClone(GloasColumnQuarantine())
      envelopeQuarantine = newClone(EnvelopeQuarantine.init())
      attestationPool = newClone(AttestationPool.init(dag, quarantine))
      elManager = new ELManager # TODO: initialise this properly
      actionTracker = default(ActionTracker)
      consensusManager = ConsensusManager.new(
        dag,
        attestationPool,
        quarantine,
        elManager,
        actionTracker,
        newClone(DynamicFeeRecipientsStore.init()),
        "",
        Opt.some default(Eth1Address),
        defaultGasLimit,
      )
      state = newClone(dag.headState)
      getTimeFn = proc(): BeaconTime =
        state[].slot.start_beacon_time(cfg.timeParams)
      batchVerifier = BatchVerifier.new(rng, taskpool)
    var
      cache: StateCache
      info: ForkedEpochInfo

    cfg.process_slots(state[], cfg.lastPremergeSlotInTestCfg, cache, info, {}).expect(
      "OK"
    )

  asyncTest "Reverse order block add & get" & preset():
    let
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
        getTimeFn)
      b1 = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      b2 = addTestBlock(state[], cache, cfg = cfg).bellatrixData

      missing = await processor.addBlock(MsgSource.gossip, b2, noSidecars)

    check:
      missing.error == VerifierError.MissingParent
      not dag.containsForkBlock(b2.root) # Unresolved, shouldn't show up
      (b2.message.slot, b2.message.proposer_index) notin
        quarantine[].seenProposerSlots

      FetchRecord(root: b1.root) in quarantine[].checkMissing(32)

    let
      status = await processor.addBlock(MsgSource.gossip, b1, noSidecars)
      b1Get = dag.getBlockRef(b1.root)

    check:
      status.isOk
      b1Get.isSome()
      dag.containsForkBlock(b1.root)
      not dag.containsForkBlock(b2.root) # Async pipeline must still run

    while processor[].hasBlocks():
      await sleepAsync(0.milliseconds)

    let b2Get = dag.getBlockRef(b2.root)

    check:
      b2Get.isSome()

      b2Get.get().parent == b1Get.get()
      (b2.message.slot, b2.message.proposer_index) in
        quarantine[].seenProposerSlots

    dag.updateHead(b2Get.get(), quarantine[], [])
    dag.pruneAtFinalization()

    # The heads structure should have been updated to contain only the new
    # b2 head
    check:
      dag.heads.mapIt(it) == @[b2Get.get()]

    # check that init also reloads block graph
    let
      validatorMonitor2 = newClone(ValidatorMonitor.init(cfg))
      dag2 = ChainDAGRef.init(cfg, db, validatorMonitor2, {})

    check:
      # ensure we loaded the correct head state
      dag2.head.root == b2.root
      dag2.headState.root == b2.message.state_root
      dag2.getBlockRef(b1.root).isSome()
      dag2.getBlockRef(b2.root).isSome()
      dag2.heads.len == 1
      dag2.heads[0].root == b2.root
      dag2.forkBlocksMatchHeads()

  asyncTest "Multiple heads" & preset():
    var
      cache2: StateCache
      cache3: StateCache
    let
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
        getTimeFn)

      # 0 <- 1a <- 2aa
      #         <- 2ab
      #   <- 1b <- 2ba
      #         <- 2bb
      s2ba = assignClone(state[])
      b1a = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      s2ab = assignClone(state[])
      b2aa = addTestBlock(
        state[], cache, graffiti = "aa".graffiti, cfg = cfg).bellatrixData
      b2ab = addTestBlock(
        s2ab[], cache3, graffiti = "ab".graffiti, cfg = cfg).bellatrixData
      b1b = addTestBlock(
        s2ba[], cache2, graffiti = "b".graffiti, cfg = cfg).bellatrixData
      s2bb = assignClone(s2ba[])
      b2ba = addTestBlock(
        s2ba[], cache2, graffiti = "ba".graffiti, cfg = cfg).bellatrixData
      b2bb = addTestBlock(
        s2bb[], cache3, graffiti = "bb".graffiti, cfg = cfg).bellatrixData
    check:
      (await processor.addBlock(MsgSource.gossip, b1a, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2aa, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2ab, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b1b, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2ba, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2bb, noSidecars)).isOk
      dag.heads.len == 4
    dag.updateHead(dag.getBlockRef(b2ab.root).get(), quarantine[], [])

    let
      validatorMonitor2 = newClone(ValidatorMonitor.init(cfg))
      dag2 = ChainDAGRef.init(cfg, db, validatorMonitor2, {})
    check:
      dag2.head.root == b2ab.root
      dag2.heads.mapIt(it.bid).toHashSet == dag.heads.mapIt(it.bid).toHashSet
      dag2.forkBlocksMatchHeads()

    let
      quarantine2 = newClone(Quarantine.init(cfg))
      pool2 = newClone(AttestationPool.init(dag2, quarantine2))
    check pool2[].forkChoiceMatchesHeads()

    for importMode in LightClientDataImportMode:
      let
        validatorMonitor3 = newClone(ValidatorMonitor.init(cfg))
        dag3 = ChainDAGRef.init(
          cfg, db, validatorMonitor3, {},
          lcDataConfig = LightClientDataConfig(
            serve: true, importMode: importMode))
      check:
        dag3.head.root == b2ab.root
        dag3.heads.len == 4
        dag3.forkBlocksMatchHeads()
        dag3.lcDataMatchesHeads()

  asyncTest "Invalidate block root" & preset():
    let
      b1 = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      b2 = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
        getTimeFn, invalidBlockRoots = @[b2.root])

    block:
      let res = await processor.addBlock(MsgSource.gossip, b2, noSidecars)
      check:
        res.isErr
        not dag.containsForkBlock(b1.root)
        not dag.containsForkBlock(b2.root)

    block:
      let res = await processor.addBlock(MsgSource.gossip, b1, noSidecars)
      check:
        res.isOk
        dag.containsForkBlock(b1.root)
        not dag.containsForkBlock(b2.root)
      while processor[].hasBlocks():
        await sleepAsync(0.milliseconds)
      check:
        dag.containsForkBlock(b1.root)
        not dag.containsForkBlock(b2.root)

    block:
      let res = await processor.addBlock(MsgSource.gossip, b2, noSidecars)
      check:
        res == Result[void, VerifierError].err VerifierError.Invalid
        dag.containsForkBlock(b1.root)
        not dag.containsForkBlock(b2.root)

  asyncTest "Invalidate existing block root" & preset():
    var
      cache2: StateCache
      cache3: StateCache
    let
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
        getTimeFn)

      # 0 <- 1a <- 2a <- 3a
      #   <- 1b <- 2ba
      #         <- 2bb
      s2ba = assignClone(state[])
      b1a = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      b2a = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      b3a = addTestBlock(state[], cache, cfg = cfg).bellatrixData
      b1b = addTestBlock(
        s2ba[], cache2, graffiti = "b".graffiti, cfg = cfg).bellatrixData
      s2bb = assignClone(s2ba[])
      b2ba = addTestBlock(
        s2ba[], cache2, graffiti = "ba".graffiti, cfg = cfg).bellatrixData
      b2bb = addTestBlock(
        s2bb[], cache3, graffiti = "bb".graffiti, cfg = cfg).bellatrixData
    check:
      (await processor.addBlock(MsgSource.gossip, b1a, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2a, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b3a, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b1b, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2ba, noSidecars)).isOk
      (await processor.addBlock(MsgSource.gossip, b2bb, noSidecars)).isOk
      dag.heads.len == 3
    dag.updateHead(dag.getBlockRef(b3a.root).get(), quarantine[], [])

    let
      validatorMonitor2 = newClone(ValidatorMonitor.init(cfg))
      dag2 = ChainDAGRef.init(
        cfg, db, validatorMonitor2, {},
        invalidBlockRoots = @[b2a.root, b2bb.root])
    check:
      dag2.head.root == b1a.root
      dag2.heads.toHashSet == toHashSet [
        dag2.getBlockRef(b1a.root).expect("Valid"),
        dag2.getBlockRef(b2ba.root).expect("Valid")]
      dag2.getForkedBlock(b1b.root).isSome
      dag2.getForkedBlock(b2a.root).isNone
      dag2.getForkedBlock(b3a.root).isNone
      dag2.getForkedBlock(b2bb.root).isNone
      dag2.forkBlocksMatchHeads()

  asyncTest "Process a block from each fork (without blobs)" & preset():
    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn)

    for consensusFork in ConsensusFork.Bellatrix .. ConsensusFork.Gloas:
      process_slots(
        cfg,
        state[],
        max(
          state[].slot + 1,
          cfg.consensusForkEpoch(consensusFork).start_slot,
        ),
        cache,
        info,
        {},
      )
      .expect("OK")

      withState(state[]):
        let b0 = addTestEngineBlock(cfg, consensusFork, forkyState, cache)
        when consensusFork == ConsensusFork.Fulu:
          let sidecarsOpt = Opt.none(fulu.DataColumnSidecarsForImport)
        else:
          let sidecarsOpt = noSidecars
        check (await processor.addBlock(
          MsgSource.gossip, b0.blck, sidecarsOpt
        )).isOk()

        when consensusFork == ConsensusFork.Gloas:
          check (await processor.addPayload(
            b0.blck, b0.envelope, Opt.none(gloas.DataColumnSidecars)
          )).isOk()

  asyncTest "Process Fulu block with data column sidecars" & preset():
    # Advance to Fulu fork
    process_slots(
      cfg, state[], start_slot(cfg.FULU_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    withState(state[]):
      when consensusFork == ConsensusFork.Fulu:
        # Create valid blobs and compute cells/proofs
        let kzgBlob = createValidKzgBlob()
        let commitment = kzg.blobToKzgCommitment(kzgBlob).valueOr:
          raiseAssert "Failed to create commitment"

        var
          computed = false
          proofs: seq[kzg.KzgProof]
        kzg.computeCellsAndKzgProofs(kzgBlob).isErrOr:
          computed = true
          proofs = value.proofs.mapIt(kzg.KzgProof(it))
        doAssert computed, "Failed to compute cells and proofs"

        # Build BlobsBundle
        let blobsBundle = testblockutil.BlobsBundle(
          commitments: @[commitment],
          proofs: proofs,
          blobs: @[kzgBlob.bytes]
        )

        # Create block with blobs
        let engineBlock = addTestEngineBlock(
          cfg, ConsensusFork.Fulu, forkyState, cache,
          blobs_bundle = blobsBundle,
        )

        # Assemble data column sidecars
        let dataColumnSidecars = assemble_data_column_sidecars(
          engineBlock.blck, @[kzgBlob], proofs, supernodeMap
        )

        # Process the block with data columns
        let res = await processor.addBlock(
          MsgSource.gossip,
          engineBlock.blck,
          # Untrusted, so this also exercises the KZG check in `storeBlock`
          Opt.some(fulu.DataColumnSidecarsForImport(
            untrusted: dataColumnSidecars))
        )

        check:
          res.isOk
          dag.containsForkBlock(engineBlock.blck.root)

  asyncTest "Process Fulu block without data column sidecars" & preset():
    # Advance to Fulu fork
    process_slots(
      cfg, state[], start_slot(cfg.FULU_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    withState(state[]):
      when consensusFork == ConsensusFork.Fulu:
        # Create block without blobs
        let engineBlock = addTestEngineBlock(cfg, ConsensusFork.Fulu, forkyState, cache)

        # Verify block has no blob commitments
        check:
          engineBlock.blck.message.body.blob_kzg_commitments.len == 0

        # Process should succeed (empty commitments is valid)
        let res = await processor.addBlock(
          MsgSource.gossip,
          engineBlock.blck,
          Opt.none(fulu.DataColumnSidecarsForImport)
        )

        check:
          res.isOk
          dag.containsForkBlock(engineBlock.blck.root)

  asyncTest "Gloas block without envelope marks missing" & preset():
    # Block arrives but envelope hasn't arrived yet.
    # Block should be stored optimistically; envelope marked as missing.
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    withState(state[]):
      when consensusFork == ConsensusFork.Gloas:
        let engineBlock = addTestEngineBlock(
          cfg, ConsensusFork.Gloas, forkyState, cache)

        let res = await processor.addBlock(
          MsgSource.gossip, engineBlock.blck, noSidecars)

        check:
          res.isOk
          dag.containsForkBlock(engineBlock.blck.root)
          # Block stored but envelope not available, should be in missing list
          FetchRecord(root: engineBlock.blck.root) in envelopeQuarantine[].checkMissing(32)

  asyncTest "Gloas block pops pre-arrived envelope from quarantine" & preset():
    # Envelope arrives before its block (orphan envelope).
    # When the block arrives, it should pop the envelope from quarantine.
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    withState(state[]):
      when consensusFork == ConsensusFork.Gloas:
        let engineBlock = addTestEngineBlock(
          cfg, ConsensusFork.Gloas, forkyState, cache)

        # Envelope arrives first and gets quarantined as orphan.
        let envelope = gloas.SignedExecutionPayloadEnvelope(
          message: gloas.ExecutionPayloadEnvelope(
            beacon_block_root: engineBlock.blck.root,
            builder_index: BUILDER_INDEX_SELF_BUILD,
            payload: gloas.ExecutionPayload(
              slot_number: engineBlock.blck.message.slot,
            )
          )
        )
        envelopeQuarantine[].addOrphan(dag.finalizedHead.slot, envelope)

        let res = await processor.addBlock(
          MsgSource.gossip, engineBlock.blck, noSidecars)

        check:
          res.isOk
          dag.containsForkBlock(engineBlock.blck.root)
          # Envelope was popped, not marked as orphan
          (engineBlock.blck.root, BUILDER_INDEX_SELF_BUILD) notin
            envelopeQuarantine[].orphans

  asyncTest "Gloas column arriving after payload import is persisted" & preset():
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let
      custodyQuarantine = newClone(GloasColumnQuarantine.init(
        cfg, supernodeMap, db.getQuarantineDB(), 12, nil))
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, custodyQuarantine, envelopeQuarantine,
        getTimeFn
      )

    withState(state[]):
      when consensusFork == ConsensusFork.Gloas:
        let
          engineBlock = addTestEngineBlock(
            cfg, ConsensusFork.Gloas, forkyState, cache)
          root = engineBlock.blck.root
          column = (ref gloas.DataColumnSidecar)(
            index: ColumnIndex(114), slot: engineBlock.blck.message.slot,
            beacon_block_root: root)

        check (await processor.addBlock(
          MsgSource.gossip, engineBlock.blck, noSidecars)).isOk
        check (await processor.addPayload(
          engineBlock.blck, engineBlock.envelope,
          Opt.none(gloas.DataColumnSidecars))).isOk

        processor.enqueueSidecars(root, [column])

        var stored: gloas.DataColumnSidecar
        check:
          db.getDataColumnSidecar(root, ColumnIndex(114), stored)
          stored.beacon_block_root == root
          not custodyQuarantine[].hasSidecar(root, ColumnIndex(114))
          FetchRecord(root: root) notin envelopeQuarantine[].checkMissing(32)

  asyncTest "Gloas columns arriving during payload import are persisted" & preset():
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let
      custodyQuarantine = newClone(GloasColumnQuarantine.init(
        cfg, supernodeMap, db.getQuarantineDB(), 12, nil))
      processor = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager, validatorMonitor,
        dataColumnQuarantine, custodyQuarantine, envelopeQuarantine,
        getTimeFn
      )

    withState(state[]):
      when consensusFork == ConsensusFork.Gloas:
        let
          engineBlock = addTestEngineBlock(
            cfg, ConsensusFork.Gloas, forkyState, cache)
          root = engineBlock.blck.root
          column = (ref gloas.DataColumnSidecar)(
            index: ColumnIndex(13), slot: engineBlock.blck.message.slot,
            beacon_block_root: root)

        check (await processor.addBlock(
          MsgSource.gossip, engineBlock.blck, noSidecars)).isOk

        # Payload not imported yet, so this one waits in the quarantine
        processor.enqueueSidecars(root, [column])
        check custodyQuarantine[].hasSidecar(root, ColumnIndex(13))

        check (await processor.addPayload(
          engineBlock.blck, engineBlock.envelope,
          Opt.none(gloas.DataColumnSidecars))).isOk

        var stored: gloas.DataColumnSidecar
        check:
          db.getDataColumnSidecar(root, ColumnIndex(13), stored)
          not custodyQuarantine[].hasSidecar(root, ColumnIndex(13))

  asyncTest "Gloas consecutive blocks accumulate missing envelopes" & preset():
    # Multiple blocks stored optimistically, each marks its envelope as missing.
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    let
      b1 = addTestBlock(state[], cache, cfg = cfg).gloasData
      b2 = addTestBlock(state[], cache, cfg = cfg).gloasData

    # Process blocks in order, both without envelopes
    let res1 = await processor.addBlock(
      MsgSource.gossip, b1, noSidecars)
    check res1.isOk

    dag.updateHead(dag.getBlockRef(b1.root).get(), quarantine[], [])

    let res2 = await processor.addBlock(
      MsgSource.gossip, b2, noSidecars)
    check res2.isOk

    # Both envelopes should be missing
    let missing = envelopeQuarantine[].checkMissing(32)
    check:
      FetchRecord(root: b1.root) in missing
      FetchRecord(root: b2.root) in missing

  asyncTest "Gloas reverse order blocks with missing parent" & preset():
    # Block N+1 arrives before block N. Block N+1 goes to
    # quarantine with MissingParent. When block N arrives and is processed,
    # block N+1 should be dequeued from quarantine.
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    let
      b1 = addTestBlock(state[], cache, cfg = cfg).gloasData
      b2 = addTestBlock(state[], cache, cfg = cfg).gloasData

    # Block N+1 arrives first, missing parent
    let res2 = await processor.addBlock(
      MsgSource.gossip, b2, noSidecars)
    check:
      res2.error == VerifierError.MissingParent
      not dag.containsForkBlock(b2.root)

    # Block N arrives, should succeed and trigger quarantine processing
    let res1 = await processor.addBlock(
      MsgSource.gossip, b1, noSidecars)
    check:
      res1.isOk
      dag.containsForkBlock(b1.root)

    # Let async quarantine processing run
    while processor[].hasBlocks():
      await sleepAsync(0.milliseconds)

    # Block N+1 should now be in the DAG (dequeued from quarantine)
    check dag.containsForkBlock(b2.root)

  asyncTest "Gloas chain with no envelopes delivered" & preset():
    # Build a chain of blocks where no envelopes are ever delivered.
    # addTestBlock creates blocks against a state without envelopes,
    # so all blocks are consistent with each other.
    process_slots(
      cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}
    ).expect("OK")

    let processor = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine,
      getTimeFn
    )

    let
      b1 = addTestBlock(state[], cache, cfg = cfg).gloasData
      b2 = addTestBlock(state[], cache, cfg = cfg).gloasData
      b3 = addTestBlock(state[], cache, cfg = cfg).gloasData
      b4 = addTestBlock(state[], cache, cfg = cfg).gloasData

    # Process b1-b3, none with envelopes.
    # Only update head to b1 so the DAG persists state at b1.
    # b2 and b3 are stored but head stays at b1.
    for blck in [b1, b2, b3]:
      let res = await processor.addBlock(
        MsgSource.gossip, blck, noSidecars)
      check res.isOk

    # Only advance head to b1
    dag.updateHead(dag.getBlockRef(b1.root).get(), quarantine[], [])

    # All envelopes missing
    check:
      dag.db.getExecutionPayloadEnvelope(b1.root).isNone
      dag.db.getExecutionPayloadEnvelope(b2.root).isNone
      dag.db.getExecutionPayloadEnvelope(b3.root).isNone

    # Simulate node restart: reinitialize DAG from the same DB.
    # All in-memory state caches are dropped, forcing updateState to
    # replay through b1-b3 slots from disk.
    let
      validatorMonitor2 = newClone(ValidatorMonitor.init(cfg))
      dag2 = ChainDAGRef.init(cfg, db, validatorMonitor2, {})
      quarantine2 = newClone(Quarantine.init(cfg))
      attestationPool2 = newClone(AttestationPool.init(dag2, quarantine2))
      consensusManager2 = ConsensusManager.new(
        dag2, attestationPool2, quarantine2, new ELManager,
        default(ActionTracker),
        newClone(DynamicFeeRecipientsStore.init()),
        "", Opt.some default(Eth1Address), defaultGasLimit)

      state2 = newClone(dag2.headState)
      getTimeFn2 = proc(): BeaconTime =
        state2[].slot.start_beacon_time(cfg.timeParams)
      processor2 = BlockProcessor.new(
        false, "", "", batchVerifier, consensusManager2, validatorMonitor2,
        newClone(FuluColumnQuarantine()), newClone(GloasColumnQuarantine()),
        newClone(EnvelopeQuarantine.init()), getTimeFn2)

    check:
      dag2.head.root == b1.root
      dag2.heads.len == 1
      dag2.heads[0].root == b3.root
      dag2.forkBlocksMatchHeads()
      attestationPool2[].forkChoiceMatchesHeads()

    # updateState should replay through b1-b3 from
    # disk, calling applyExecutionPayloadEnvelope for each
    let res4 = await processor2.addBlock(
      MsgSource.gossip, b4, noSidecars)
    check res4.isOk


  template finalizedColumnsFixture(body: untyped) {.dirty.} =
    process_slots(cfg, state[], start_slot(cfg.GLOAS_FORK_EPOCH),
      cache, info, {}).expect("Gloas fixture")
    let processor {.inject.} = BlockProcessor.new(
      false, "", "", batchVerifier, consensusManager, validatorMonitor,
      dataColumnQuarantine, gloasColumnQuarantine, envelopeQuarantine, getTimeFn)
    withState(state[]):
      when consensusFork == ConsensusFork.Gloas:
        let
          blob = createValidKzgBlob()
          commitment = kzg.blobToKzgCommitment(blob).expect("fixture commitment")
          cellsAndProofs = kzg.computeCellsAndKzgProofs(blob).expect("fixture cells")
          proofs = cellsAndProofs.proofs.mapIt(kzg.KzgProof(it))
          bundle = testblockutil.BlobsBundle(
            commitments: @[commitment], proofs: proofs, blobs: @[blob.bytes])
          engine {.inject.} = addTestEngineBlock(
            cfg, ConsensusFork.Gloas, forkyState, cache, blobs_bundle = bundle)
          columns {.inject.} = assemble_data_column_sidecars(
            engine.blck, @[blob], proofs, supernodeMap)
          bid {.inject.} = engine.blck.toBlockId()
        require columns.len == NUMBER_OF_COLUMNS.int
        require (await processor.addBlock(
          MsgSource.sync, engine.blck, noSidecars)).isOk
        dag.updateHead(dag.getBlockRef(bid.root).get(), quarantine[], [])
        # Unit fixture finality boundary: the block itself passed native import.
        # This models finalized import without a network/finality simulation.
        dag.finalizedHead = BlockSlot(blck: dag.head, slot: bid.slot)
        body

  asyncTest "Finalized Gloas duplicate envelope repairs columns after envelope-only commit" & preset():
    finalizedColumnsFixture:
      var wakes: seq[Slot]
      processor.onDataColumnsStored = proc(slot: Slot) = wakes.add(slot)
      require (await processor.addPayload(
        engine.blck, engine.envelope, Opt.none(gloas.DataColumnSidecars))).isOk
      let saved = SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get())
      check not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
      check (await processor.addPayload(engine.blck, engine.envelope,
        Opt.some(columns[0..0]))).isOk
      check:
        db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
        SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get()) == saved
        wakes == @[bid.slot]
      let repeat = await processor.addPayload(
        engine.blck, engine.envelope, Opt.some(columns[0..0]))
      check repeat.isErr and repeat.error == PayloadVerifierError.Duplicate
      check wakes == @[bid.slot]

  for envelopePresent in [false, true]:
    asyncTest "Finalized Gloas column DB accumulates below quorum E=" & $envelopePresent & preset():
      finalizedColumnsFixture:
        if envelopePresent:
          require (await processor.addPayload(engine.blck, engine.envelope,
            Opt.none(gloas.DataColumnSidecars))).isOk
        var wakes: seq[Slot]
        processor.onDataColumnsStored = proc(slot: Slot) = wakes.add(slot)
        for limits in [(0, 0), (1, 62), (63, 63), (64, 126), (127, 127)]:
          require processor.addBackfillSidecars(bid, columns[limits[0]..limits[1]]).isOk
          for i in 0..limits[1]:
            check db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(i))
          check db.containsExecutionPayloadEnvelope(bid.root) == envelopePresent
          # Quarantine finalization cannot discard already committed DB columns.
          gloasColumnQuarantine[].pruneAfterFinalization(
            bid.slot.epoch + 1, Opt.none(Slot))
        let before = wakes.len
        check processor.addBackfillSidecars(bid, columns).isOk
        check wakes.len == before
        for wake in wakes:
          check wake == bid.slot

  asyncTest "Finalized Gloas invalid column batch has zero writes or wakeup" & preset():
    finalizedColumnsFixture:
      var wakes: seq[Slot]
      processor.onDataColumnsStored = proc(slot: Slot) = wakes.add(slot)
      for kind in 0..6:
        var bad = newClone(copyFixture(columns[1][]))
        case kind
        of 0: bad.slot = bid.slot + 1
        of 1: bad.beacon_block_root = ZERO_HASH
        of 2: bad.index = ColumnIndex(NUMBER_OF_COLUMNS)
        of 3: bad.column.setLen(0)
        of 4: bad.kzg_proofs[0] = default(typeof(bad.kzg_proofs[0]))
        of 5: bad = columns[0] # duplicate index
        else: bad = nil
        let batch = @[columns[0], bad]
        let res = processor.addBackfillSidecars(bid, batch)
        check res.isErr
        check (await processor.addPayload(engine.blck, engine.envelope,
          Opt.some(batch))).isErr
        check:
          not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
          not db.containsExecutionPayloadEnvelope(bid.root)
          wakes.len == 0

  for envelopePresent in [false, true]:
    asyncTest "Finalized Gloas invalid envelope zero writes E=" & $envelopePresent & preset():
      finalizedColumnsFixture:
        if envelopePresent:
          require (await processor.addPayload(engine.blck, engine.envelope,
            Opt.none(gloas.DataColumnSidecars))).isOk
        let saved =
          if envelopePresent: SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get())
          else: newSeq[byte]()
        for kind in 0..6:
          var bad = copyFixture(engine.envelope)
          case kind
          of 0: bad.message.payload.block_hash = ZERO_HASH
          of 1: bad.message.payload.gas_used += 1
          of 2: bad.message.builder_index = BuilderIndex(0)
          of 3: bad.signature = default(ValidatorSig)
          of 4: bad.message.beacon_block_root = ZERO_HASH
          of 5: bad.message.payload.transactions[0].setLen(0)
          else: bad.message.parent_beacon_block_root = ZERO_HASH
          let res = await processor.addPayload(engine.blck, bad, Opt.some(columns[0..0]))
          check:
            res.isErr
            db.containsExecutionPayloadEnvelope(bid.root) == envelopePresent
            not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
          if envelopePresent:
            check SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get()) == saved

  asyncTest "Finalized Gloas missed slot alias is refused exactly" & preset():
    finalizedColumnsFixture:
      dag.finalizedHead.slot = bid.slot + 1
      var bad = copyFixture(engine.envelope)
      bad.message.payload.slot_number = bid.slot + 1
      check dag.addBackfillExecutionPayload(bad).isErr
      check processor.addBackfillSidecars(
        BlockId(root: bid.root, slot: bid.slot + 1), columns[0..0]).isErr
      check:
        not db.containsExecutionPayloadEnvelope(bid.root)
        not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))

  asyncTest "Finalized Gloas mismatched canonical block-envelope pair has zero writes" & preset():
    finalizedColumnsFixture:
      let other = addTestEngineBlock(cfg, ConsensusFork.Gloas, forkyState,
        cache, blobs_bundle = engine.blobsBundle)
      require (await processor.addBlock(MsgSource.sync, other.blck, noSidecars)).isOk
      dag.updateHead(dag.getBlockRef(other.blck.root).get(), quarantine[], [])
      dag.finalizedHead = BlockSlot(blck: dag.head, slot: other.blck.message.slot)
      let mismatch = await processor.addPayload(
        engine.blck, other.envelope, Opt.some(columns[0..0]))
      check:
        mismatch.isErr
        not db.containsExecutionPayloadEnvelope(other.blck.root)
        not db.containsExecutionPayloadEnvelope(bid.root)
        not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
      # The rejected envelope is valid for its OWN canonical block.
      check dag.addBackfillExecutionPayload(other.envelope).isOk

  asyncTest "Finalized Gloas conflicting stored envelope and columns are preserved" & preset():
    finalizedColumnsFixture:
      var conflicting = copyFixture(engine.envelope)
      conflicting.message.payload.gas_used += 1
      db.putExecutionPayloadEnvelope(conflicting)
      let saved = SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get())
      let res = await processor.addPayload(engine.blck, engine.envelope, Opt.some(columns[0..0]))
      check:
        res.isErr and res.error == PayloadVerifierError.LocalVerification
        SSZ.encode(db.getExecutionPayloadEnvelope(bid.root).get()) == saved
        not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(0))
      # Independent valid column repair does not need the conflicting envelope.
      check processor.addBackfillSidecars(bid, columns[0..0]).isOk
      var conflictColumn = newClone(copyFixture(columns[1][]))
      conflictColumn.slot = bid.slot + 1
      db.putDataColumnSidecars(@[conflictColumn])
      var stored: gloas.DataColumnSidecar
      require db.getDataColumnSidecar(bid.root, ColumnIndex(1), stored)
      let savedColumn = SSZ.encode(stored)
      let cres = processor.addBackfillSidecars(bid, @[columns[2], columns[1]])
      check:
        cres.isErr and cres.error == PayloadVerifierError.LocalVerification
        not db.containsDataColumnSidecar(ConsensusFork.Gloas, bid.root, ColumnIndex(2))
      require db.getDataColumnSidecar(bid.root, ColumnIndex(1), stored)
      check SSZ.encode(stored) == savedColumn

  asyncTest "Finalized Gloas reused head builder key refuses locally before Duplicate" & preset():
    finalizedColumnsFixture:
      # Model a stored finalized external-builder block with native signatures.
      # This fixture targets historical import, not a replayed finality chain.
      var historical = copyFixture(engine.blck)
      historical.message.body.signed_execution_payload_bid.message.builder_index = BuilderIndex(0)
      historical.message.body.signed_execution_payload_bid.signature =
        get_execution_payload_bid_signature(forkyState.data.fork,
          dag.genesis_validators_root, bid.slot.epoch,
          historical.message.body.signed_execution_payload_bid.message,
          MockPrivKeys[0'u64]).toValidatorSig()
      historical.root = hash_tree_root(historical.message)
      historical.signature = get_block_signature(forkyState.data.fork,
        dag.genesis_validators_root, bid.slot, historical.root,
        MockPrivKeys[historical.message.proposer_index]).toValidatorSig()
      let forkedHistorical = ForkedSignedBeaconBlock.init(historical)
      db.putBlock(forkedHistorical.asTrusted().gloasData)
      dag.head = BlockRef.init(historical.root, OptimisticStatus.valid, historical.message)
      dag.finalizedHead = BlockSlot(blck: dag.head, slot: bid.slot)
      dag.headState.gloasData.data.builders.add(gloas.Builder(
        pubkey: MockPubKeys[0'u64], version: PAYLOAD_BUILDER_VERSION,
        withdrawable_epoch: FAR_FUTURE_EPOCH))
      var envelope = copyFixture(engine.envelope)
      envelope.message.builder_index = BuilderIndex(0)
      envelope.message.beacon_block_root = historical.root
      envelope.signature = get_execution_payload_envelope_signature(
        forkyState.data.fork, dag.genesis_validators_root,
        bid.slot.epoch, envelope.message, MockPrivKeys[0'u64]).toValidatorSig()
      require dag.addBackfillExecutionPayload(envelope).isOk
      let saved = SSZ.encode(db.getExecutionPayloadEnvelope(historical.root).get())
      dag.headState.gloasData.data.builders.mitem(BuilderIndex(0)).pubkey = MockPubKeys[1'u64]
      let refused = dag.addBackfillExecutionPayload(envelope)
      check:
        refused.isErr and refused.error == PayloadVerifierError.LocalVerification
        SSZ.encode(db.getExecutionPayloadEnvelope(historical.root).get()) == saved

# Clean up KZG trusted setup at the end of all tests
doAssert kzg.freeTrustedSetup().isOk
