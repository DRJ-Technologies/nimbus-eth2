# beacon_chain
# Copyright (c) 2024-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [], gcsafe.}

import
  std/[typetraits, sequtils],
  eth/common/eth_types_rlp,
  ./[helpers, state_transition_block]

func readExecutionTransaction(
    txBytes: bellatrix.Transaction): Result[EthTransaction, string] =
  try:
    ok rlp.decode(distinctBase(txBytes), EthTransaction)
  except RlpError as exc:
    err("Invalid transaction: " & exc.msg)

func readExecutionTransaction(
    txBytes: gloas.Transaction): Result[EthTransaction, string] =
  try:
    ok rlp.decode(distinctBase(txBytes), EthTransaction)
  except RlpError as exc:
    err("Invalid transaction: " & exc.msg)

func all_blob_versioned_hashes*(
    transactions: seq[bellatrix.Transaction] | seq[gloas.Transaction]
): Result[seq[Hash32], string] =
  var hashes: seq[Hash32]
  for txBytes in transactions:
    if txBytes.len == 0 or txBytes[0] != TxEip4844.byte:
      continue
    let tx = ? txBytes.readExecutionTransaction()
    for vHash in tx.versionedHashes:
      hashes.add vHash
  ok hashes

# https://github.com/ethereum/consensus-specs/blob/v1.5.0-alpha.4/specs/deneb/beacon-chain.md#is_valid_versioned_hashes
func is_valid_versioned_hashes*(
    blck: deneb.BeaconBlock | electra.BeaconBlock | fulu.BeaconBlock |
          gloas.BeaconBlock,
    envelope: NoEnvelope | gloas.ExecutionPayloadEnvelope,
): Result[void, string] =
  const consensusFork = typeof(blck).kind

  template transactions: untyped =
    when consensusFork >= ConsensusFork.Gloas:
      envelope.payload.transactions
    else:
      blck.body.execution_payload.transactions
  template commitments: untyped =
    when consensusFork >= ConsensusFork.Gloas:
      blck.body.signed_execution_payload_bid.message.blob_kzg_commitments
    else:
      blck.body.blob_kzg_commitments

  var i = 0
  for txBytes in transactions:
    if txBytes.len == 0 or txBytes[0] != TxEip4844.byte:
      continue  # Only blob transactions may have blobs
    let tx = ? txBytes.readExecutionTransaction()
    for vHash in tx.versionedHashes:
      if commitments.len <= i:
        return err("Extra blobs without matching `blob_kzg_commitments`")
      if vHash != kzg_commitment_to_versioned_hash(commitments[i]):
        return err("Invalid `blob_versioned_hash` at index " & $i)
      inc i
  if i != commitments.len:
    return err("Extra `blob_kzg_commitments` without matching blobs")
  ok()

func is_valid_versioned_hashes*(
    blck: deneb.BeaconBlock | electra.BeaconBlock | fulu.BeaconBlock |
          gloas.BeaconBlock,
): Result[void, string] =
  is_valid_versioned_hashes(blck, noEnvelope)

proc verifyPayloadOnCL*(
    signedBlock: bellatrix.SignedBeaconBlock | capella.SignedBeaconBlock |
                 deneb.SignedBeaconBlock | electra.SignedBeaconBlock |
                 fulu.SignedBeaconBlock | gloas.SignedBeaconBlock |
                 heze.SignedBeaconBlock,
    signedEnvelope: NoEnvelope | gloas.SignedExecutionPayloadEnvelope):
    Result[void, string] =
  const consensusFork = typeof(signedBlock).kind
  # When the execution layer is not available to verify the payload, we do the
  # required checks on the CL instead and proceed as if the EL was syncing
  # https://github.com/ethereum/consensus-specs/blob/v1.6.0-alpha.6/specs/bellatrix/beacon-chain.md#verify_and_notify_new_payload
  # https://github.com/ethereum/consensus-specs/blob/v1.6.0-alpha.6/specs/deneb/beacon-chain.md#modified-verify_and_notify_new_payload

  template payload(): auto =
    when consensusFork >= ConsensusFork.Gloas:
      signedEnvelope.message.payload
    else:
      signedBlock.message.body.execution_payload

  if payload.transactions.anyIt(it.len == 0):
    return err("Execution block contains zero length transactions")

  template computedBlockHash(): auto =
    when consensusFork >= ConsensusFork.Gloas:
      signedEnvelope.message.compute_execution_block_hash()
    else:
      signedBlock.message.compute_execution_block_hash()
  if payload.block_hash != computedBlockHash:
    return err("Execution block hash validation failed")

  # [New in Deneb:EIP4844]
  when consensusFork >= ConsensusFork.Deneb:
    let blobsRes =
      when consensusFork >= ConsensusFork.Gloas:
        signedBlock.message.is_valid_versioned_hashes(signedEnvelope.message)
      else:
        signedBlock.message.is_valid_versioned_hashes()
    if blobsRes.isErr:
      return err("Blob versioned hashes invalid, reason:" & $blobsRes.error)
  else:
    # If there are EIP-4844 (type 3) transactions in the payload with
    # versioned hashes, the transactions would be rejected by the EL
    # based on payload timestamp (only allowed post Deneb);
    # There are no `blob_kzg_commitments` before Deneb to compare against
    discard

  ok()
