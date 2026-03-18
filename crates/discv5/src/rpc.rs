use alloy_rlp::{Decodable, Error as DecoderError};
use enr::{CombinedKey, Enr};
use std::{
    convert::TryInto,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::NonZeroU16,
};
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Protobuf encoding helpers
// ---------------------------------------------------------------------------

pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
    buf
}

pub fn encode_field_varint(field_num: u32, value: u64) -> Vec<u8> {
    let key = (field_num as u64) << 3; // wire type 0
    let mut buf = encode_varint(key);
    buf.extend_from_slice(&encode_varint(value));
    buf
}

pub fn encode_field_bytes(field_num: u32, data: &[u8]) -> Vec<u8> {
    let key = ((field_num as u64) << 3) | 2; // wire type 2
    let mut buf = encode_varint(key);
    buf.extend_from_slice(&encode_varint(data.len() as u64));
    buf.extend_from_slice(data);
    buf
}

/// Wraps protobuf fields with a varint length prefix (matching Nim's `initProtoBuffer`/`finish()`).
fn encode_proto_message(fields: &[u8]) -> Vec<u8> {
    // No length prefix — raw protobuf fields concatenated.
    // The Archivist's initProtoBuffer().finish() does NOT add a length prefix.
    fields.to_vec()
}

// ---------------------------------------------------------------------------
// Protobuf decoding helpers
// ---------------------------------------------------------------------------

fn decode_varint(data: &[u8]) -> Result<(u64, usize), DecoderError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 70 {
            return Err(DecoderError::Custom("Varint too long"));
        }
        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(DecoderError::Custom("Unexpected end of varint"))
}

/// Decode one protobuf field from `data`.
/// Returns `(field_num, wire_type, field_data_slice, total_bytes_consumed)`.
///
/// For wire type 0 (varint), `field_data_slice` contains the raw varint bytes.
/// For wire type 2 (length-delimited), `field_data_slice` contains the payload bytes.
fn decode_field(data: &[u8]) -> Result<(u32, u8, Vec<u8>, usize), DecoderError> {
    let (key, key_len) = decode_varint(data)?;
    let wire_type = (key & 0x07) as u8;
    let field_num = (key >> 3) as u32;
    let rest = &data[key_len..];
    match wire_type {
        0 => {
            // varint
            let (val, val_len) = decode_varint(rest)?;
            let raw = encode_varint(val);
            Ok((field_num, wire_type, raw, key_len + val_len))
        }
        2 => {
            // length-delimited
            let (length, len_len) = decode_varint(rest)?;
            let length = length as usize;
            let start = len_len;
            let end = start + length;
            if end > rest.len() {
                return Err(DecoderError::Custom(
                    "Length-delimited field extends beyond data",
                ));
            }
            Ok((
                field_num,
                wire_type,
                rest[start..end].to_vec(),
                key_len + end,
            ))
        }
        _ => Err(DecoderError::Custom("Unsupported protobuf wire type")),
    }
}

/// Parse all fields from a protobuf buffer into a vec of (field_num, wire_type, data).
fn decode_all_fields(data: &[u8]) -> Result<Vec<(u32, u8, Vec<u8>)>, DecoderError> {
    let mut fields = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let (field_num, wire_type, field_data, consumed) = decode_field(&data[offset..])?;
        fields.push((field_num, wire_type, field_data));
        offset += consumed;
    }
    Ok(fields)
}

/// Extract the varint value from raw varint bytes.
fn varint_value(data: &[u8]) -> Result<u64, DecoderError> {
    let (val, _) = decode_varint(data)?;
    Ok(val)
}

// ---------------------------------------------------------------------------
// IP address protobuf helpers (Archivist format)
// ---------------------------------------------------------------------------

/// Encode an IP address as a nested protobuf message:
/// `{ field 1 (bytes): family_byte, field 2 (bytes): ip_bytes }`
fn encode_ip_proto(ip: &IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => {
            let mut inner = encode_field_bytes(1, &[4u8]); // family = IPv4
            inner.extend_from_slice(&encode_field_bytes(2, &v4.octets()));
            encode_proto_message(&inner)
        }
        IpAddr::V6(v6) => {
            let mut inner = encode_field_bytes(1, &[6u8]); // family = IPv6
            inner.extend_from_slice(&encode_field_bytes(2, &v6.octets()));
            encode_proto_message(&inner)
        }
    }
}

/// Decode an IP address from the nested protobuf bytes.
fn decode_ip_proto(data: &[u8]) -> Result<IpAddr, DecoderError> {
    let fields = decode_all_fields(data)?;
    let mut family: Option<u8> = None;
    let mut ip_bytes: Option<Vec<u8>> = None;
    for (fnum, _wt, fdata) in &fields {
        match fnum {
            1 => {
                if fdata.len() != 1 {
                    return Err(DecoderError::Custom("Invalid IP family byte"));
                }
                family = Some(fdata[0]);
            }
            2 => {
                ip_bytes = Some(fdata.clone());
            }
            _ => {} // skip unknown fields
        }
    }
    let family = family.ok_or(DecoderError::Custom("Missing IP family field"))?;
    let ip_bytes = ip_bytes.ok_or(DecoderError::Custom("Missing IP bytes field"))?;
    match family {
        4 => {
            if ip_bytes.len() != 4 {
                return Err(DecoderError::Custom("Invalid IPv4 address length"));
            }
            let mut octets = [0u8; 4];
            octets.copy_from_slice(&ip_bytes);
            Ok(IpAddr::V4(Ipv4Addr::from(octets)))
        }
        6 => {
            if ip_bytes.len() != 16 {
                return Err(DecoderError::Custom("Invalid IPv6 address length"));
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&ip_bytes);
            let v6 = Ipv6Addr::from(octets);
            if v6.is_loopback() {
                Ok(IpAddr::V6(v6))
            } else if let Some(v4) = v6.to_ipv4() {
                Ok(IpAddr::V4(v4))
            } else {
                Ok(IpAddr::V6(v6))
            }
        }
        _ => Err(DecoderError::Custom("Unknown IP family")),
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Type to manage the request IDs.
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct RequestId(pub Vec<u8>);

impl From<RequestId> for Vec<u8> {
    fn from(id: RequestId) -> Self {
        id.0
    }
}

impl RequestId {
    /// Decodes the ID from raw bytes.
    pub fn decode(data: Vec<u8>) -> Result<Self, DecoderError> {
        if data.len() > 8 {
            return Err(DecoderError::Custom("Invalid ID length"));
        }
        Ok(RequestId(data))
    }

    pub fn random() -> Self {
        let rand: u64 = rand::random();
        RequestId(rand.to_be_bytes().to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A combined type representing requests and responses.
pub enum Message {
    /// A request, which contains its [`RequestId`].
    Request(Request),
    /// A Response, which contains the [`RequestId`] of its associated request.
    Response(Response),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request sent between nodes.
pub struct Request {
    /// The [`RequestId`] of the request.
    pub id: RequestId,
    /// The body of the request.
    pub body: RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A response sent in response to a [`Request`]
pub struct Response {
    /// The [`RequestId`] of the request that triggered this response.
    pub id: RequestId,
    /// The body of this response.
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestBody {
    /// A PING request.
    Ping {
        /// Our current ENR sequence number.
        enr_seq: u64,
    },
    /// A FINDNODE request.
    FindNode {
        /// The distance(s) of peers we expect to be returned in the response.
        distances: Vec<u64>,
    },
    /// A Talk request.
    Talk {
        /// The protocol requesting.
        protocol: Vec<u8>,
        /// The request.
        request: Vec<u8>,
    },
    /// An AddProvider request.
    AddProvider {
        /// Content ID (32 bytes, NodeId big-endian).
        content_id: Vec<u8>,
        /// SignedPeerRecord raw bytes.
        provider_record: Vec<u8>,
    },
    /// A GetProviders request.
    GetProviders {
        /// Content ID (32 bytes, NodeId big-endian).
        content_id: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseBody {
    /// A PONG response.
    Pong {
        /// The current ENR sequence number of the responder.
        enr_seq: u64,
        /// Our external IP address as observed by the responder.
        ip: IpAddr,
        /// Our external UDP port as observed by the responder.
        port: NonZeroU16,
    },
    /// A NODES response.
    Nodes {
        /// The total number of responses that make up this response.
        total: u64,
        /// A list of ENR's returned by the responder.
        nodes: Vec<Enr<CombinedKey>>,
    },
    /// The TALK response.
    Talk {
        /// The response for the talk.
        response: Vec<u8>,
    },
    /// A PROVIDERS response.
    Providers {
        /// The total number of responses that make up this response.
        total: u32,
        /// A list of SignedPeerRecord raw bytes.
        providers: Vec<Vec<u8>>,
    },
}

// ---------------------------------------------------------------------------
// Encoding helpers for building the inner body protobuf
// ---------------------------------------------------------------------------

fn encode_ping_body(enr_seq: u64) -> Vec<u8> {
    let fields = encode_field_varint(1, enr_seq);
    encode_proto_message(&fields)
}

fn encode_pong_body(enr_seq: u64, ip: &IpAddr, port: NonZeroU16) -> Vec<u8> {
    let mut fields = encode_field_varint(1, enr_seq);
    let ip_proto = encode_ip_proto(ip);
    fields.extend_from_slice(&encode_field_bytes(2, &ip_proto));
    let port_bytes = port.get().to_be_bytes();
    fields.extend_from_slice(&encode_field_bytes(3, &port_bytes));
    encode_proto_message(&fields)
}

fn encode_findnode_body(distances: &[u64]) -> Vec<u8> {
    let mut fields = Vec::new();
    for &d in distances {
        let d16 = (d as u16).to_be_bytes();
        fields.extend_from_slice(&encode_field_bytes(1, &d16));
    }
    encode_proto_message(&fields)
}

fn encode_nodes_body(total: u64, nodes: &[Enr<CombinedKey>]) -> Vec<u8> {
    let mut fields = encode_field_varint(1, total);
    for node in nodes {
        let enr_bytes = alloy_rlp::encode(node);
        fields.extend_from_slice(&encode_field_bytes(2, &enr_bytes));
    }
    encode_proto_message(&fields)
}

fn encode_talkreq_body(protocol: &[u8], request: &[u8]) -> Vec<u8> {
    let mut fields = encode_field_bytes(1, protocol);
    fields.extend_from_slice(&encode_field_bytes(2, request));
    encode_proto_message(&fields)
}

fn encode_talkresp_body(response: &[u8]) -> Vec<u8> {
    // NOTE: TalkResp uses field 2, not field 1
    let fields = encode_field_bytes(2, response);
    encode_proto_message(&fields)
}

fn encode_addprovider_body(content_id: &[u8], provider_record: &[u8]) -> Vec<u8> {
    let mut fields = encode_field_bytes(1, content_id);
    fields.extend_from_slice(&encode_field_bytes(2, provider_record));
    encode_proto_message(&fields)
}

fn encode_getproviders_body(content_id: &[u8]) -> Vec<u8> {
    let fields = encode_field_bytes(1, content_id);
    encode_proto_message(&fields)
}

fn encode_providers_body(total: u32, providers: &[Vec<u8>]) -> Vec<u8> {
    let mut fields = encode_field_varint(1, u64::from(total));
    for prov in providers {
        fields.extend_from_slice(&encode_field_bytes(2, prov));
    }
    encode_proto_message(&fields)
}

// ---------------------------------------------------------------------------
// Envelope encoding: [msg_type][envelope_proto]
// Envelope proto: length-prefixed { field1: request_id, field2: inner_body }
// ---------------------------------------------------------------------------

fn encode_envelope(msg_type: u8, request_id: &[u8], inner_body: &[u8]) -> Vec<u8> {
    let mut envelope_fields = encode_field_bytes(1, request_id);
    envelope_fields.extend_from_slice(&encode_field_bytes(2, inner_body));
    let envelope = encode_proto_message(&envelope_fields);
    let mut buf = Vec::with_capacity(1 + envelope.len());
    buf.push(msg_type);
    buf.extend_from_slice(&envelope);
    buf
}

/// Decode envelope: extract field 1 (request_id) and field 2 (body).
/// No length prefix — raw protobuf fields.
fn decode_envelope(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), DecoderError> {
    let fields = decode_all_fields(data)?;
    let mut request_id: Option<Vec<u8>> = None;
    let mut body: Option<Vec<u8>> = None;
    for (fnum, _wt, fdata) in fields {
        match fnum {
            1 => request_id = Some(fdata),
            2 => body = Some(fdata),
            _ => {} // skip unknown fields
        }
    }
    let request_id = request_id.ok_or(DecoderError::Custom("Missing request ID in envelope"))?;
    let body = body.unwrap_or_default();
    Ok((request_id, body))
}

// ---------------------------------------------------------------------------
// SPR (SignedPeerRecord) to ENR conversion
// ---------------------------------------------------------------------------

/// Decode an Archivist SPR (protobuf SignedEnvelope) into an ENR with correct NodeId.
///
/// The SPR is verified by parsing its libp2p SignedEnvelope structure and
/// extracting the secp256k1 public key and addresses. The resulting ENR
/// has `externally_verified = true` with the correct NodeId derived from
/// the SPR's public key via keccak256.
pub fn enr_from_spr_bytes(spr_bytes: &[u8]) -> Option<Enr<CombinedKey>> {
    // Parse the SignedEnvelope protobuf:
    // field 1 = public_key (protobuf: {field 1: key_type, field 2: key_data})
    // field 2 = payload_type
    // field 3 = payload (PeerRecord protobuf)
    // field 5 = signature
    let envelope_fields = decode_all_fields(spr_bytes).ok()?;

    let mut public_key_proto: Option<Vec<u8>> = None;
    let mut payload: Option<Vec<u8>> = None;

    for (fnum, _wt, fdata) in &envelope_fields {
        match fnum {
            1 => public_key_proto = Some(fdata.clone()),
            3 => payload = Some(fdata.clone()),
            _ => {}
        }
    }

    let pk_proto = public_key_proto?;
    let payload_bytes = payload?;

    // Parse PublicKey protobuf: field 1 = key_type (1 = Secp256k1), field 2 = data
    let pk_fields = decode_all_fields(&pk_proto).ok()?;
    let mut key_type: u64 = 0;
    let mut key_data: Option<Vec<u8>> = None;
    for (fnum, _wt, fdata) in &pk_fields {
        match fnum {
            1 => key_type = varint_value(fdata).unwrap_or(0),
            2 => key_data = Some(fdata.clone()),
            _ => {}
        }
    }

    // Secp256k1 = 2 in libp2p's protobuf KeyType enum
    if key_type != 2 {
        debug!("SPR has non-secp256k1 key type: {}", key_type);
        return None;
    }
    let secp_key_bytes = key_data?;

    // Parse PeerRecord protobuf to extract addresses:
    // field 1 = peer_id, field 2 = seq, field 3 (repeated) = AddressInfo
    let pr_fields = decode_all_fields(&payload_bytes).ok()?;
    let mut seq: u64 = 0;
    let mut ipv4: Option<Ipv4Addr> = None;
    let mut udp_port: Option<u16> = None;
    let mut tcp_port: Option<u16> = None;

    for (fnum, wt, fdata) in &pr_fields {
        match (*fnum, *wt) {
            (2, 0) => seq = varint_value(fdata).unwrap_or(0),
            (3, 2) => {
                // AddressInfo: field 1 = multiaddr bytes
                if let Ok(addr_fields) = decode_all_fields(fdata) {
                    for (afnum, _awt, adata) in &addr_fields {
                        if *afnum == 1 {
                            if let Some((ip, port, proto)) = parse_multiaddr(adata) {
                                ipv4 = Some(ip);
                                if proto == "udp" {
                                    udp_port = Some(port);
                                } else if proto == "tcp" {
                                    tcp_port = Some(port);
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Build ENR with correct NodeId from verified SPR public key
    match Enr::<CombinedKey>::from_verified_spr(&secp_key_bytes, seq, ipv4, udp_port, tcp_port) {
        Ok(enr) => {
            debug!(
                "Decoded SPR: NodeId={}, ip={:?}, udp={:?}",
                enr.node_id(),
                ipv4,
                udp_port
            );
            Some(enr)
        }
        Err(e) => {
            debug!("Failed to build ENR from SPR: {:?}", e);
            None
        }
    }
}

/// Parse a raw multiaddr binary, returning (ip, port, protocol).
fn parse_multiaddr(data: &[u8]) -> Option<(Ipv4Addr, u16, &'static str)> {
    if data.len() < 7 {
        return None;
    }
    // /ip4 = protocol code 0x04
    if data[0] != 0x04 {
        return None;
    }
    let ip = Ipv4Addr::new(data[1], data[2], data[3], data[4]);

    // Next protocol: UDP (0x0111 = 273, varint 0x91 0x02) or TCP (0x06)
    if data.len() >= 9 && data[5] == 0x91 && data[6] == 0x02 {
        let port = u16::from_be_bytes([data[7], data[8]]);
        return Some((ip, port, "udp"));
    }
    if data.len() >= 8 && data[5] == 0x06 {
        let port = u16::from_be_bytes([data[6], data[7]]);
        return Some((ip, port, "tcp"));
    }
    None
}

// ---------------------------------------------------------------------------
// Decode inner body helpers
// ---------------------------------------------------------------------------

/// Decode a length-prefixed protobuf body into fields.
fn decode_body_fields(data: &[u8]) -> Result<Vec<(u32, u8, Vec<u8>)>, DecoderError> {
    // No length prefix — raw protobuf fields.
    decode_all_fields(data)
}

// ---------------------------------------------------------------------------
// Request impl
// ---------------------------------------------------------------------------

impl Request {
    pub fn msg_type(&self) -> u8 {
        match self.body {
            RequestBody::Ping { .. } => 0x01,
            RequestBody::FindNode { .. } => 0x03,
            RequestBody::Talk { .. } => 0x05,
            RequestBody::AddProvider { .. } => 0x0B,
            RequestBody::GetProviders { .. } => 0x0C,
        }
    }

    /// Encodes a Request to protobuf-encoded bytes.
    pub fn encode(self) -> Vec<u8> {
        let msg_type = self.msg_type();
        let id_bytes = self.id.as_bytes();
        let inner_body = match &self.body {
            RequestBody::Ping { enr_seq } => encode_ping_body(*enr_seq),
            RequestBody::FindNode { distances } => encode_findnode_body(distances),
            RequestBody::Talk { protocol, request } => encode_talkreq_body(protocol, request),
            RequestBody::AddProvider {
                content_id,
                provider_record,
            } => encode_addprovider_body(content_id, provider_record),
            RequestBody::GetProviders { content_id } => encode_getproviders_body(content_id),
        };
        let encoded = encode_envelope(msg_type, id_bytes, &inner_body);
        debug!(
            msg_type = msg_type,
            body_hex = hex::encode(&inner_body),
            envelope_hex = hex::encode(&encoded),
            "Encoded request"
        );
        encoded
    }
}

// ---------------------------------------------------------------------------
// Response impl
// ---------------------------------------------------------------------------

impl Response {
    pub fn msg_type(&self) -> u8 {
        match &self.body {
            ResponseBody::Pong { .. } => 0x02,
            ResponseBody::Nodes { .. } => 0x04,
            ResponseBody::Talk { .. } => 0x06,
            ResponseBody::Providers { .. } => 0x0D,
        }
    }

    /// Determines if the response is a valid response to the given request.
    pub fn match_request(&self, req: &RequestBody) -> bool {
        match self.body {
            ResponseBody::Pong { .. } => matches!(req, RequestBody::Ping { .. }),
            ResponseBody::Nodes { .. } => matches!(req, RequestBody::FindNode { .. }),
            ResponseBody::Talk { .. } => matches!(req, RequestBody::Talk { .. }),
            ResponseBody::Providers { .. } => matches!(req, RequestBody::GetProviders { .. }),
        }
    }

    /// Encodes a Response to protobuf-encoded bytes.
    pub fn encode(self) -> Vec<u8> {
        let msg_type = self.msg_type();
        let id_bytes = self.id.as_bytes();
        let inner_body = match &self.body {
            ResponseBody::Pong { enr_seq, ip, port } => encode_pong_body(*enr_seq, ip, *port),
            ResponseBody::Nodes { total, nodes } => encode_nodes_body(*total, nodes),
            ResponseBody::Talk { response } => encode_talkresp_body(response),
            ResponseBody::Providers { total, providers } => {
                encode_providers_body(*total, providers)
            }
        };
        encode_envelope(msg_type, id_bytes, &inner_body)
    }
}

// ---------------------------------------------------------------------------
// Display impls
// ---------------------------------------------------------------------------

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Request(request) => write!(f, "{request}"),
            Message::Response(response) => write!(f, "{response}"),
        }
    }
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response: id: {}: {}", self.id, self.body)
    }
}

impl std::fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseBody::Pong { enr_seq, ip, port } => {
                write!(f, "PONG: Enr-seq: {enr_seq}, Ip: {ip:?},  Port: {port}")
            }
            ResponseBody::Nodes { total, nodes } => {
                write!(f, "NODES: total: {total}, Nodes: [")?;
                let mut first = true;
                for id in nodes {
                    if !first {
                        write!(f, ", {id}")?;
                    } else {
                        write!(f, "{id}")?;
                    }
                    first = false;
                }
                write!(f, "]")
            }
            ResponseBody::Talk { response } => {
                write!(f, "Response: Response {}", hex::encode(response))
            }
            ResponseBody::Providers { total, providers } => {
                write!(
                    f,
                    "PROVIDERS: total: {total}, providers: [{}]",
                    providers
                        .iter()
                        .map(hex::encode)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request: id: {}: {}", self.id, self.body)
    }
}

impl std::fmt::Display for RequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestBody::Ping { enr_seq } => write!(f, "PING: enr_seq: {enr_seq}"),
            RequestBody::FindNode { distances } => {
                write!(f, "FINDNODE Request: distance: {distances:?}")
            }
            RequestBody::Talk { protocol, request } => write!(
                f,
                "TALK: protocol: {}, request: {}",
                hex::encode(protocol),
                hex::encode(request)
            ),
            RequestBody::AddProvider {
                content_id,
                provider_record,
            } => write!(
                f,
                "ADDPROVIDER: content_id: {}, provider_record: {} bytes",
                hex::encode(content_id),
                provider_record.len()
            ),
            RequestBody::GetProviders { content_id } => {
                write!(f, "GETPROVIDERS: content_id: {}", hex::encode(content_id))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message encode / decode
// ---------------------------------------------------------------------------

impl Message {
    pub fn encode(self) -> Vec<u8> {
        match self {
            Self::Request(request) => request.encode(),
            Self::Response(response) => response.encode(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self, DecoderError> {
        if data.len() < 3 {
            return Err(DecoderError::InputTooShort);
        }

        let msg_type = data[0];
        let (request_id_bytes, body_bytes) = decode_envelope(&data[1..])?;
        let id = RequestId::decode(request_id_bytes)?;
        let body_fields = decode_body_fields(&body_bytes)?;

        let message = match msg_type {
            0x01 => {
                // Ping
                let mut enr_seq: u64 = 0;
                for (fnum, wt, fdata) in &body_fields {
                    if *fnum == 1 && *wt == 0 {
                        enr_seq = varint_value(fdata)?;
                    }
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::Ping { enr_seq },
                })
            }
            0x02 => {
                // Pong
                let mut enr_seq: u64 = 0;
                let mut ip_data: Option<Vec<u8>> = None;
                let mut port_data: Option<Vec<u8>> = None;
                for (fnum, wt, fdata) in &body_fields {
                    match (*fnum, *wt) {
                        (1, 0) => enr_seq = varint_value(fdata)?,
                        (2, 2) => ip_data = Some(fdata.clone()),
                        (3, 2) => port_data = Some(fdata.clone()),
                        _ => {}
                    }
                }
                let ip_data =
                    ip_data.ok_or(DecoderError::Custom("Missing IP field in Pong"))?;
                let ip = decode_ip_proto(&ip_data)?;
                let port_data =
                    port_data.ok_or(DecoderError::Custom("Missing port field in Pong"))?;
                if port_data.len() != 2 {
                    return Err(DecoderError::Custom("Invalid port length in Pong"));
                }
                let raw_port = u16::from_be_bytes([port_data[0], port_data[1]]);
                let port: NonZeroU16 = raw_port
                    .try_into()
                    .map_err(|_| DecoderError::Custom("PONG response port number invalid"))?;
                Message::Response(Response {
                    id,
                    body: ResponseBody::Pong { enr_seq, ip, port },
                })
            }
            0x03 => {
                // FindNode
                let mut distances = Vec::new();
                for (fnum, wt, fdata) in &body_fields {
                    if *fnum == 1 && *wt == 2 {
                        if fdata.len() != 2 {
                            return Err(DecoderError::Custom(
                                "Invalid distance length in FindNode",
                            ));
                        }
                        let d = u16::from_be_bytes([fdata[0], fdata[1]]) as u64;
                        if d > 256 {
                            warn!(
                                distance = d,
                                "Rejected FindNode request asking for unknown distance maximum 256",
                            );
                            return Err(DecoderError::Custom(
                                "FINDNODE request distance invalid",
                            ));
                        }
                        distances.push(d);
                    }
                }
                Message::Request(Request {
                    id,
                    body: RequestBody::FindNode { distances },
                })
            }
            0x04 => {
                // Nodes — Archivist sends SPR records (protobuf SignedPeerRecords),
                // not RLP-encoded ENRs. Try ENR decode first, skip on failure.
                let mut total: u64 = 0;
                let mut nodes = Vec::new();
                for (fnum, wt, fdata) in &body_fields {
                    match (*fnum, *wt) {
                        (1, 0) => total = varint_value(fdata)?,
                        (2, 2) => {
                            match Enr::<CombinedKey>::decode(&mut &fdata[..]) {
                                Ok(enr) => nodes.push(enr),
                                Err(_) => {
                                    // Archivist SPR record — can't decode as ENR.
                                    // Try to extract enough info to create a synthetic ENR.
                                    if let Some(enr) = enr_from_spr_bytes(fdata) {
                                        debug!("Decoded SPR record as synthetic ENR");
                                        nodes.push(enr);
                                    } else {
                                        debug!(
                                            "Skipping non-ENR record in Nodes response ({} bytes)",
                                            fdata.len()
                                        );
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::Nodes { total, nodes },
                })
            }
            0x05 => {
                // TalkReq
                let mut protocol: Option<Vec<u8>> = None;
                let mut request: Option<Vec<u8>> = None;
                for (fnum, wt, fdata) in &body_fields {
                    if *wt == 2 {
                        match *fnum {
                            1 => protocol = Some(fdata.clone()),
                            2 => request = Some(fdata.clone()),
                            _ => {}
                        }
                    }
                }
                let protocol = protocol
                    .ok_or(DecoderError::Custom("Missing protocol field in TalkReq"))?;
                let request =
                    request.ok_or(DecoderError::Custom("Missing request field in TalkReq"))?;
                Message::Request(Request {
                    id,
                    body: RequestBody::Talk { protocol, request },
                })
            }
            0x06 => {
                // TalkResp - field 2, not field 1
                let mut response: Vec<u8> = Vec::new();
                for (fnum, wt, fdata) in &body_fields {
                    if *fnum == 2 && *wt == 2 {
                        response = fdata.clone();
                    }
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::Talk { response },
                })
            }
            0x0B => {
                // AddProvider
                let mut content_id: Option<Vec<u8>> = None;
                let mut provider_record: Option<Vec<u8>> = None;
                for (fnum, wt, fdata) in &body_fields {
                    if *wt == 2 {
                        match *fnum {
                            1 => content_id = Some(fdata.clone()),
                            2 => provider_record = Some(fdata.clone()),
                            _ => {}
                        }
                    }
                }
                let content_id = content_id.ok_or(DecoderError::Custom(
                    "Missing content_id in AddProvider",
                ))?;
                let provider_record = provider_record.ok_or(DecoderError::Custom(
                    "Missing provider_record in AddProvider",
                ))?;
                Message::Request(Request {
                    id,
                    body: RequestBody::AddProvider {
                        content_id,
                        provider_record,
                    },
                })
            }
            0x0C => {
                // GetProviders
                let mut content_id: Option<Vec<u8>> = None;
                for (fnum, wt, fdata) in &body_fields {
                    if *fnum == 1 && *wt == 2 {
                        content_id = Some(fdata.clone());
                    }
                }
                let content_id = content_id.ok_or(DecoderError::Custom(
                    "Missing content_id in GetProviders",
                ))?;
                Message::Request(Request {
                    id,
                    body: RequestBody::GetProviders { content_id },
                })
            }
            0x0D => {
                // Providers
                let mut total: u32 = 0;
                let mut providers = Vec::new();
                for (fnum, wt, fdata) in &body_fields {
                    match (*fnum, *wt) {
                        (1, 0) => total = varint_value(fdata)? as u32,
                        (2, 2) => providers.push(fdata.clone()),
                        _ => {}
                    }
                }
                Message::Response(Response {
                    id,
                    body: ResponseBody::Providers { total, providers },
                })
            }
            _ => {
                return Err(DecoderError::Custom("Unknown RPC message type"));
            }
        };

        Ok(message)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_varint_small() {
        assert_eq!(encode_varint(0), vec![0x00]);
        assert_eq!(encode_varint(1), vec![0x01]);
        assert_eq!(encode_varint(127), vec![0x7F]);
    }

    #[test]
    fn encode_varint_multi_byte() {
        assert_eq!(encode_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_varint(300), vec![0xAC, 0x02]);
    }

    #[test]
    fn decode_varint_roundtrip() {
        for val in [0u64, 1, 127, 128, 255, 256, 300, 65535, u64::MAX] {
            let encoded = encode_varint(val);
            let (decoded, consumed) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn encode_decode_ping_request() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::Ping { enr_seq: 15 },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_ping_request_zero_seq() {
        let id = RequestId(vec![42]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::Ping { enr_seq: 0 },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_ping_request_large_seq() {
        let id = RequestId(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::Ping {
                enr_seq: u64::MAX,
            },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_pong_response_ipv4() {
        let id = RequestId(vec![1]);
        let response = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: "127.0.0.1".parse().unwrap(),
                port: 80.try_into().unwrap(),
            },
        });
        let encoded = response.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn encode_decode_pong_response_ipv6() {
        let id = RequestId(vec![7]);
        let response = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 100,
                ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                port: 9000.try_into().unwrap(),
            },
        });
        let encoded = response.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn encode_decode_pong_response_ipv4_mapped() {
        let id = RequestId(vec![1]);
        let response = Message::Response(Response {
            id: id.clone(),
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: IpAddr::V6(Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped()),
                port: 80.try_into().unwrap(),
            },
        });
        let encoded = response.encode();
        let decoded = Message::decode(&encoded).unwrap();
        // IPv4-mapped IPv6 should decode as IPv4
        let expected = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 15,
                ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
                port: 80.try_into().unwrap(),
            },
        });
        assert_eq!(expected, decoded);
    }

    #[test]
    fn encode_decode_findnode_request_single() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::FindNode {
                distances: vec![12],
            },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn encode_decode_findnode_request_multiple() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::FindNode {
                distances: vec![0, 1, 128, 256],
            },
        });
        let encoded = request.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn decode_findnode_rejects_invalid_distance() {
        let id = RequestId(vec![1]);
        let request = Message::Request(Request {
            id,
            body: RequestBody::FindNode {
                distances: vec![257],
            },
        });
        let encoded = request.encode();
        Message::decode(&encoded).expect_err("distance 257 should be rejected");
    }

    #[test]
    fn encode_decode_nodes_response_empty() {
        let id = RequestId(vec![1]);
        let response = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![],
            },
        });
        let encoded = response.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn encode_decode_nodes_response_single() {
        let key = CombinedKey::generate_secp256k1();
        let enr = Enr::builder()
            .ip4("127.0.0.1".parse().unwrap())
            .udp4(500)
            .build(&key)
            .unwrap();
        let id = RequestId(vec![1]);
        let response = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![enr],
            },
        });
        let encoded = response.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn encode_decode_nodes_response_multiple() {
        let key = CombinedKey::generate_secp256k1();
        let enr1 = Enr::builder()
            .ip4("127.0.0.1".parse().unwrap())
            .udp4(500)
            .build(&key)
            .unwrap();
        let enr2 = Enr::builder()
            .ip4("10.0.0.1".parse().unwrap())
            .tcp4(8080)
            .build(&key)
            .unwrap();
        let enr3 = Enr::builder()
            .ip("10.4.5.6".parse().unwrap())
            .build(&key)
            .unwrap();
        let id = RequestId(vec![1]);
        let response = Message::Response(Response {
            id,
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![enr1, enr2, enr3],
            },
        });
        let encoded = response.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn encode_decode_talk_request() {
        let id = RequestId(vec![113, 236, 255, 66, 31, 191, 221, 86]);
        let message = Message::Request(Request {
            id,
            body: RequestBody::Talk {
                protocol: hex::decode("757470").unwrap(),
                request: hex::decode("0100a028839e1549000003ef001000007619dde7").unwrap(),
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_talk_response() {
        let id = RequestId(vec![113, 236, 255, 66, 31, 191, 221, 86]);
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Talk {
                response: hex::decode("0100a028839e1549000003ef001000007619dde7").unwrap(),
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_talk_response_empty() {
        let id = RequestId(vec![0]);
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Talk {
                response: vec![],
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_addprovider_request() {
        let id = RequestId(vec![1, 2, 3]);
        let content_id = vec![0xAA; 32];
        let provider_record = vec![0xBB; 64];
        let message = Message::Request(Request {
            id,
            body: RequestBody::AddProvider {
                content_id: content_id.clone(),
                provider_record: provider_record.clone(),
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_getproviders_request() {
        let id = RequestId(vec![5, 6]);
        let content_id = vec![0xCC; 32];
        let message = Message::Request(Request {
            id,
            body: RequestBody::GetProviders {
                content_id: content_id.clone(),
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_providers_response() {
        let id = RequestId(vec![7, 8]);
        let providers = vec![vec![0xDD; 48], vec![0xEE; 48]];
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Providers {
                total: 2,
                providers,
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn encode_decode_providers_response_empty() {
        let id = RequestId(vec![1]);
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Providers {
                total: 0,
                providers: vec![],
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn decode_rejects_too_short_data() {
        let data = [0x01, 0x02];
        Message::decode(&data).expect_err("should reject short data");
    }

    #[test]
    fn decode_rejects_unknown_msg_type() {
        // Build a valid-looking envelope with unknown type 0xFF
        let inner_body = encode_ping_body(1);
        let encoded = encode_envelope(0xFF, &[1], &inner_body);
        Message::decode(&encoded).expect_err("should reject unknown msg type");
    }

    #[test]
    fn decode_rejects_invalid_request_id_length() {
        // RequestId longer than 8 bytes
        let inner_body = encode_ping_body(1);
        let encoded = encode_envelope(0x01, &[1, 2, 3, 4, 5, 6, 7, 8, 9], &inner_body);
        Message::decode(&encoded).expect_err("should reject request id > 8 bytes");
    }

    #[test]
    fn match_request_ping_pong() {
        let resp = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Pong {
                enr_seq: 1,
                ip: "127.0.0.1".parse().unwrap(),
                port: 80.try_into().unwrap(),
            },
        };
        assert!(resp.match_request(&RequestBody::Ping { enr_seq: 1 }));
        assert!(!resp.match_request(&RequestBody::FindNode {
            distances: vec![1]
        }));
    }

    #[test]
    fn match_request_nodes_findnode() {
        let resp = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![],
            },
        };
        assert!(resp.match_request(&RequestBody::FindNode {
            distances: vec![1]
        }));
        assert!(!resp.match_request(&RequestBody::Ping { enr_seq: 1 }));
    }

    #[test]
    fn match_request_talk() {
        let resp = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Talk {
                response: vec![1, 2, 3],
            },
        };
        assert!(resp.match_request(&RequestBody::Talk {
            protocol: vec![],
            request: vec![]
        }));
        assert!(!resp.match_request(&RequestBody::Ping { enr_seq: 1 }));
    }

    #[test]
    fn match_request_providers_getproviders() {
        let resp = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Providers {
                total: 1,
                providers: vec![],
            },
        };
        assert!(resp.match_request(&RequestBody::GetProviders {
            content_id: vec![0; 32]
        }));
        assert!(!resp.match_request(&RequestBody::Ping { enr_seq: 1 }));
        assert!(!resp.match_request(&RequestBody::FindNode {
            distances: vec![1]
        }));
    }

    #[test]
    fn display_request_id() {
        let id = RequestId(vec![0xAB, 0xCD]);
        assert_eq!(format!("{id}"), "abcd");
    }

    #[test]
    fn display_request_body_ping() {
        let body = RequestBody::Ping { enr_seq: 42 };
        let s = format!("{body}");
        assert!(s.contains("PING"));
        assert!(s.contains("42"));
    }

    #[test]
    fn display_request_body_addprovider() {
        let body = RequestBody::AddProvider {
            content_id: vec![0xAA; 4],
            provider_record: vec![0xBB; 8],
        };
        let s = format!("{body}");
        assert!(s.contains("ADDPROVIDER"));
        assert!(s.contains("aaaaaaaa"));
    }

    #[test]
    fn display_request_body_getproviders() {
        let body = RequestBody::GetProviders {
            content_id: vec![0xCC; 4],
        };
        let s = format!("{body}");
        assert!(s.contains("GETPROVIDERS"));
        assert!(s.contains("cccccccc"));
    }

    #[test]
    fn display_response_body_providers() {
        let body = ResponseBody::Providers {
            total: 2,
            providers: vec![vec![0xDD; 2], vec![0xEE; 2]],
        };
        let s = format!("{body}");
        assert!(s.contains("PROVIDERS"));
        assert!(s.contains("2"));
        assert!(s.contains("dddd"));
        assert!(s.contains("eeee"));
    }

    #[test]
    fn msg_type_request_variants() {
        let ping = Request {
            id: RequestId(vec![1]),
            body: RequestBody::Ping { enr_seq: 1 },
        };
        assert_eq!(ping.msg_type(), 0x01);

        let findnode = Request {
            id: RequestId(vec![1]),
            body: RequestBody::FindNode {
                distances: vec![1],
            },
        };
        assert_eq!(findnode.msg_type(), 0x03);

        let talk = Request {
            id: RequestId(vec![1]),
            body: RequestBody::Talk {
                protocol: vec![],
                request: vec![],
            },
        };
        assert_eq!(talk.msg_type(), 0x05);

        let addprov = Request {
            id: RequestId(vec![1]),
            body: RequestBody::AddProvider {
                content_id: vec![],
                provider_record: vec![],
            },
        };
        assert_eq!(addprov.msg_type(), 0x0B);

        let getprov = Request {
            id: RequestId(vec![1]),
            body: RequestBody::GetProviders {
                content_id: vec![],
            },
        };
        assert_eq!(getprov.msg_type(), 0x0C);
    }

    #[test]
    fn msg_type_response_variants() {
        let pong = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Pong {
                enr_seq: 1,
                ip: "127.0.0.1".parse().unwrap(),
                port: 80.try_into().unwrap(),
            },
        };
        assert_eq!(pong.msg_type(), 0x02);

        let nodes = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Nodes {
                total: 1,
                nodes: vec![],
            },
        };
        assert_eq!(nodes.msg_type(), 0x04);

        let talk = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Talk {
                response: vec![],
            },
        };
        assert_eq!(talk.msg_type(), 0x06);

        let providers = Response {
            id: RequestId(vec![1]),
            body: ResponseBody::Providers {
                total: 0,
                providers: vec![],
            },
        };
        assert_eq!(providers.msg_type(), 0x0D);
    }

    #[test]
    fn protobuf_field_encoding_bytes() {
        // Field 1, wire type 2 (length-delimited), data [0xAA, 0xBB]
        let encoded = encode_field_bytes(1, &[0xAA, 0xBB]);
        // Key = (1 << 3) | 2 = 0x0A, length = 2, data = AA BB
        assert_eq!(encoded, vec![0x0A, 0x02, 0xAA, 0xBB]);
    }

    #[test]
    fn protobuf_field_encoding_varint() {
        // Field 1, wire type 0 (varint), value 150
        let encoded = encode_field_varint(1, 150);
        // Key = (1 << 3) | 0 = 0x08, value 150 = 0x96 0x01
        assert_eq!(encoded, vec![0x08, 0x96, 0x01]);
    }

    #[test]
    fn proto_message_has_no_length_prefix() {
        let fields = vec![0x08, 0x01]; // field 1, varint 1
        let msg = encode_proto_message(&fields);
        assert_eq!(msg, vec![0x08, 0x01]);
    }

    #[test]
    fn decode_all_fields_roundtrip() {
        let mut data = encode_field_varint(1, 42);
        data.extend_from_slice(&encode_field_bytes(2, &[0xDE, 0xAD]));
        let fields = decode_all_fields(&data).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, 1); // field_num
        assert_eq!(fields[0].1, 0); // wire_type varint
        assert_eq!(varint_value(&fields[0].2).unwrap(), 42);
        assert_eq!(fields[1].0, 2); // field_num
        assert_eq!(fields[1].1, 2); // wire_type bytes
        assert_eq!(fields[1].2, vec![0xDE, 0xAD]);
    }

    #[test]
    fn encode_decode_ip_proto_v4() {
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let encoded = encode_ip_proto(&ip);
        let decoded = decode_ip_proto(&encoded).unwrap();
        assert_eq!(ip, decoded);
    }

    #[test]
    fn encode_decode_ip_proto_v6() {
        let ip = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let encoded = encode_ip_proto(&ip);
        let decoded = decode_ip_proto(&encoded).unwrap();
        assert_eq!(ip, decoded);
    }

    #[test]
    fn pong_port_5000() {
        let id = RequestId(vec![1]);
        let message = Message::Response(Response {
            id,
            body: ResponseBody::Pong {
                enr_seq: 1,
                ip: "127.0.0.1".parse().unwrap(),
                port: 5000.try_into().unwrap(),
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn findnode_distance_256() {
        let id = RequestId(vec![1]);
        let message = Message::Request(Request {
            id,
            body: RequestBody::FindNode {
                distances: vec![256],
            },
        });
        let encoded = message.clone().encode();
        let decoded = Message::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn request_id_random_roundtrip() {
        let id = RequestId::random();
        assert!(id.0.len() <= 8);
        let id2 = RequestId::decode(id.0.clone()).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn encode_message_via_message_enum() {
        let msg = Message::Request(Request {
            id: RequestId(vec![1]),
            body: RequestBody::Ping { enr_seq: 1 },
        });
        let encoded_via_enum = msg.clone().encode();
        let encoded_via_request = match msg {
            Message::Request(r) => r.encode(),
            _ => unreachable!(),
        };
        assert_eq!(encoded_via_enum, encoded_via_request);
    }
}
