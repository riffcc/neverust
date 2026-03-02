## FFI bridge for embedding the real Archivist node (Nim) into Rust tests.
## This exposes a tiny C ABI for start/poll/stop so Rust can run Archivist
## without shelling out to a subprocess.

{.push raises: [].}

import std/os
import std/options
import std/strutils

import pkg/chronos
import pkg/confutils/defs
import pkg/libp2p
import pkg/libp2p/routing_record
import pkg/questionable
import pkg/questionable/results

import archivist/conf
import archivist/archivist
import archivist/utils/fileutils
import archivist/utils/keyutils

type
  ArchivistFfiNode = ref object
    server: NodeServer
    lastError: string

var pinnedNodes: seq[ArchivistFfiNode] = @[]
var globalLastError: string

proc pinNode(node: ArchivistFfiNode) =
  pinnedNodes.add(node)

proc unpinNode(nodePtr: pointer) =
  for i in 0 ..< pinnedNodes.len:
    if cast[pointer](pinnedNodes[i]) == nodePtr:
      pinnedNodes.delete(i)
      break

proc parseBootstrapSprs(value: cstring): seq[SignedPeerRecord] =
  discard value
  @[]

proc buildNodeConfig(
    dataDir: string,
    apiBindAddr: string,
    apiPort: uint16,
    discPort: uint16,
    listenPort: uint16,
    bootstrapNodes: seq[SignedPeerRecord],
): NodeConf =
  let maybeListenAddress = MultiAddress.init("/ip4/0.0.0.0/tcp/" & $listenPort)
  let listenAddrs =
    if maybeListenAddress.isOk:
      @[maybeListenAddress.get()]
    else:
      @[]

  NodeConf(
    logLevel: "info",
    logFormat: LogKind.NoColors,
    metricsEnabled: false,
    metricsAddress: static parseIpAddress("127.0.0.1"),
    metricsPort: Port(8008),
    dataDir: OutDir(dataDir),
    listenAddrs: listenAddrs,
    nat: defaultNatConfig(),
    discoveryPort: Port(discPort),
    netPrivKeyFile: "key",
    bootstrapNodes: bootstrapNodes,
    maxPeers: 160,
    numThreads: ThreadCount(0),
    agentString: "Archivist Node",
    apiBindAddress: apiBindAddr,
    apiPort: Port(apiPort),
    apiCorsAllowedOrigin: none(string),
    repoKind: repoFS,
    fsDirectIO: false,
    fsFsyncFile: true,
    fsFsyncDir: true,
    storageQuota: DefaultQuotaBytes,
    overlayTtl: DefaultOverlayTtl.seconds,
    blockMaintenanceInterval: DefaultBlockInterval,
    blockMaintenanceNumberOfBlocks: DefaultNumBlocksPerInterval,
    cacheSize: NBytes(0),
    logFile: none(string),
    persistence: false,
    ethProvider: "ws://localhost:8545",
    ethPrivateKey: none(string),
    marketplaceAddress: none(EthAddress),
    useSystemClock: false,
    validator: false,
    validatorMaxSlots: MaxSlots(1000),
    validatorGroups: none(int),
    validatorGroupIndex: 0'u16,
    marketplaceRequestCacheSize: DefaultRequestCacheSize,
    maxPriorityFeePerGas: DefaultMaxPriorityFeePerGas,
    prover: false,
    circuitDir: OutDir(defaultCircuitDir()),
    proverBackend: ProverBackendCmd.nimgroth16,
    curve: Curves.bn128,
    circomR1cs: InputFile(defaultCircuitDir() / "proof_main.r1cs"),
    circomGraph: InputFile(defaultCircuitDir() / "proof_main.bin"),
    circomWasm: InputFile(defaultCircuitDir() / "proof_main.wasm"),
    circomZkey: InputFile(defaultCircuitDir() / "proof_main.zkey"),
    circomNoZkey: false,
  )

proc archivist_ffi_start*(
    data_dir: cstring,
    api_bindaddr: cstring,
    api_port: uint16,
    disc_port: uint16,
    listen_port: uint16,
    bootstrap_sprs: cstring,
): pointer {.exportc, dynlib, cdecl.} =
  try:
    let dataDir =
      if data_dir.isNil:
        ""
      else:
        ($data_dir).strip()
    if dataDir.len == 0:
      globalLastError = "data_dir must not be empty"
      return nil

    let apiBindAddr =
      if api_bindaddr.isNil:
        "127.0.0.1"
      else:
        ($api_bindaddr).strip()
    if apiBindAddr.len == 0:
      globalLastError = "api_bindaddr must not be empty"
      return nil

    createDir(dataDir)
    if not checkAndCreateDataDir(dataDir):
      globalLastError = "Unable to initialize data directory: " & dataDir
      return nil
    if not checkAndCreateDataDir(dataDir / "repo"):
      globalLastError = "Unable to initialize repo directory: " & (dataDir / "repo")
      return nil

    let bootstrapNodes = parseBootstrapSprs(bootstrap_sprs)
    let config = buildNodeConfig(
      dataDir = dataDir,
      apiBindAddr = apiBindAddr,
      apiPort = api_port,
      discPort = disc_port,
      listenPort = listen_port,
      bootstrapNodes = bootstrapNodes,
    )

    config.setupLogging()
    config.setupMetrics()

    let keyPath = dataDir / config.netPrivKeyFile
    without privateKey =? setupKey(keyPath), err:
      globalLastError = "Unable to initialize network private key: " & err.msg
      return nil

    let server = NodeServer.new(config, privateKey)
    waitFor server.start()

    let node = ArchivistFfiNode(server: server, lastError: "")
    pinNode(node)
    globalLastError = ""
    return cast[pointer](node)
  except Exception as exc:
    globalLastError = exc.msg
    return nil

proc archivist_ffi_poll*() {.exportc, dynlib, cdecl.} =
  try:
    chronos.poll()
  except Exception as exc:
    globalLastError = exc.msg

proc archivist_ffi_stop*(node_ptr: pointer): cint {.exportc, dynlib, cdecl.} =
  if node_ptr.isNil:
    globalLastError = "Cannot stop Archivist node: null handle"
    return 1

  let node = cast[ArchivistFfiNode](node_ptr)
  if node.isNil:
    globalLastError = "Cannot stop Archivist node: invalid handle"
    return 1

  try:
    waitFor node.server.stop()
    unpinNode(node_ptr)
    globalLastError = ""
    return 0
  except Exception as exc:
    node.lastError = exc.msg
    globalLastError = exc.msg
    unpinNode(node_ptr)
    return 1

proc archivist_ffi_last_error*(node_ptr: pointer): cstring {.exportc, dynlib, cdecl.} =
  if node_ptr.isNil:
    return globalLastError.cstring

  let node = cast[ArchivistFfiNode](node_ptr)
  if node.isNil:
    return globalLastError.cstring

  if node.lastError.len > 0:
    return node.lastError.cstring

  globalLastError.cstring
