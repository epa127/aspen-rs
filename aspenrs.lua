------------------------------------------------------------
-- AspenRS Protocol Dissector
-- Layout:
--   MessageHeader: kind: u8, payload_len: u64 (big endian)
--   PayloadHeader: req_id: u64
------------------------------------------------------------

local SERVER_PORT = 12345  -- set this to your server's TCP port

local BE_BYTE       = 6
local LC_READ_BYTE  = 7
local LC_WRITE_BYTE = 8
local NONE_BYTE     = 0
local SOME_BYTE     = 1

local LEN_LENGTH      = 8               -- u64
local MSG_HDR_LEN     = 1 + LEN_LENGTH  -- kind:1 + payload_len:8
local PAYLOAD_HDR_LEN = LEN_LENGTH      -- req_id:8

------------------------------------------------------------
-- Protocol and fields
------------------------------------------------------------

local aspenrs = Proto("aspenrs", "AspenRS Protocol")

local type_vals = {
    [BE_BYTE]       = "BeRead",
    [LC_READ_BYTE]  = "LcRead",
    [LC_WRITE_BYTE] = "LcWrite",
}

local f_type    = ProtoField.uint8("aspenrs.type", "Type", base.DEC, type_vals)
local f_len     = ProtoField.uint64("aspenrs.payload_len", "Payload Length", base.DEC)
local f_req_id  = ProtoField.uint64("aspenrs.req_id", "Request ID", base.DEC)

-- Request fields
local f_req_key       = ProtoField.uint64("aspenrs.request.id", "Key", base.DEC)
local f_req_substring = ProtoField.string("aspenrs.request.substring", "Substring")
local f_req_username  = ProtoField.string("aspenrs.request.username", "Username")

-- Response fields
local f_resp_freq         = ProtoField.uint64("aspenrs.response.freq", "Frequency", base.DEC)
local f_resp_has_username = ProtoField.uint8(
    "aspenrs.response.has_username",
    "Username Present",
    base.DEC,
    { [NONE_BYTE] = "None", [SOME_BYTE] = "Some" }
)
local f_resp_username     = ProtoField.string("aspenrs.response.username", "Username")

aspenrs.fields = {
    f_type, f_len, f_req_id,
    f_req_key, f_req_substring, f_req_username,
    f_resp_freq, f_resp_has_username, f_resp_username,
}

------------------------------------------------------------
-- Helper: direction (request vs response)
------------------------------------------------------------

local function is_request(pinfo)
    -- convention: packets to SERVER_PORT are requests
    return pinfo.dst_port == SERVER_PORT
end

------------------------------------------------------------
-- Dissect a single full message (buffer is exactly 1 PDU)
------------------------------------------------------------

local function dissect_message(buffer, pinfo, tree, kind, payload_len)
    local pktlen = buffer:len()
    local full_len = MSG_HDR_LEN + payload_len

    if pktlen < full_len then
        local subtree = tree:add(aspenrs, buffer())
        subtree:add_expert_info(
            PI_MALFORMED, PI_ERROR,
            string.format("Incomplete message: need %d, have %d", full_len, pktlen)
        )
        return
    end

    -- Top-level subtree covers one entire message
    local subtree = tree:add(aspenrs, buffer(0, full_len))

    local kind_range = buffer(0,1)
    local len_range  = buffer(1,LEN_LENGTH)

    subtree:add(f_type, kind_range)
    subtree:add(f_len,  len_range)

    local payload = buffer(MSG_HDR_LEN, payload_len)

    -- PayloadHeader: req_id
    if payload_len < PAYLOAD_HDR_LEN then
        subtree:add_expert_info(
            PI_MALFORMED, PI_ERROR,
            string.format("Payload too short for req_id: have %d, need %d",
                          payload_len, PAYLOAD_HDR_LEN)
        )
        return
    end

    local req_id_range = payload(0, PAYLOAD_HDR_LEN)
    local req_id       = req_id_range:uint64():tonumber()
    subtree:add(f_req_id, req_id_range)

    local body_len = payload_len - PAYLOAD_HDR_LEN
    local body     = payload(PAYLOAD_HDR_LEN, body_len)

    local dir_is_req = is_request(pinfo)
    local dir_str    = dir_is_req and "Request" or "Response"
    local type_str   = type_vals[kind] or ("Unknown(" .. tostring(kind) .. ")")

    pinfo.cols.protocol = "ASPENRS"
    pinfo.cols.info     = string.format(
        "%s %s req_id=%d",
        dir_str, type_str, req_id
    )

    --------------------------------------------------------
    -- Requests
    --------------------------------------------------------
    if dir_is_req then
        local req_tree = subtree:add(aspenrs, payload, "Request Payload")

        if kind == BE_BYTE then
            -- BeRead Request: body = substring bytes (>=1)
            if body_len >= 1 then
                local substring = body:string()
                req_tree:add(f_req_substring, body, substring)
                pinfo.cols.info:append(' substring="' .. substring .. '"')
            else
                req_tree:add_expert_info(
                    PI_MALFORMED, PI_ERROR,
                    "BeRead request: substring missing"
                )
            end

        elseif kind == LC_READ_BYTE then
            -- LcRead Request: body = id: u64 (exactly 8)
            if body_len ~= 8 then
                req_tree:add_expert_info(
                    PI_MALFORMED, PI_ERROR,
                    "LcRead request: body must be exactly 8 bytes (u64 id)"
                )
            else
                local id_val = body:uint64():tonumber()
                req_tree:add(f_req_key, body)
                pinfo.cols.info:append(" id=" .. tostring(id_val))
            end

        elseif kind == LC_WRITE_BYTE then
            -- LcWrite Request: body = id: u64 + username bytes (>= 8)
            if body_len < 8 then
                req_tree:add_expert_info(
                    PI_MALFORMED, PI_ERROR,
                    "LcWrite request: body must be at least 8 bytes (id + username)"
                )
            else
                local id_range = body(0,8)
                local id_val   = id_range:uint64():tonumber()
                req_tree:add(f_req_key, id_range)

                local uname_len = body_len - 8
                if uname_len > 0 then
                    local uname_range = body(8, uname_len)
                    local uname       = uname_range:string()
                    req_tree:add(f_req_username, uname_range, uname)
                    pinfo.cols.info:append(
                        string.format(" id=%d username=\"%s\"", id_val, uname)
                    )
                else
                    -- matches your Rust behavior for empty username
                    pinfo.cols.info:append(
                        string.format(" id=%d username=<empty>", id_val)
                    )
                end
            end

        else
            req_tree:add_expert_info(
                PI_UNDECODED, PI_WARN,
                "Unknown request type: " .. tostring(kind)
            )
        end

    --------------------------------------------------------
    -- Responses
    --------------------------------------------------------
    else
        local resp_tree = subtree:add(aspenrs, payload, "Response Payload")

        if kind == BE_BYTE then
            -- BeRead Response: body = freq: u64 (exactly 8)
            if body_len ~= 8 then
                resp_tree:add_expert_info(
                    PI_MALFORMED, PI_ERROR,
                    "BeRead response: body must be exactly 8 bytes (u64 freq)"
                )
            else
                local freq_val = body:uint64():tonumber()
                resp_tree:add(f_resp_freq, body)
                pinfo.cols.info:append(" freq=" .. tostring(freq_val))
            end

        elseif kind == LC_READ_BYTE or kind == LC_WRITE_BYTE then
            -- LcRead/LcWrite Response:
            -- body[0] = 0 (NONE) or 1 (SOME)
            -- if SOME, remainder = username bytes (may be empty)
            if body_len < 1 then
                resp_tree:add_expert_info(
                    PI_MALFORMED, PI_ERROR,
                    "Response: body too short for option tag"
                )
            else
                local tag_range = body(0,1)
                local tag       = tag_range:uint()
                resp_tree:add(f_resp_has_username, tag_range)

                if tag == NONE_BYTE then
                    pinfo.cols.info:append(" username=None")

                elseif tag == SOME_BYTE then
                    local uname_len = body_len - 1
                    if uname_len > 0 then
                        local uname_range = body(1, uname_len)
                        local uname       = uname_range:string()
                        resp_tree:add(f_resp_username, uname_range, uname)
                        pinfo.cols.info:append(' username="' .. uname .. '"')
                    else
                        -- matches your Rust: SOME + no bytes → empty string
                        resp_tree:add(f_resp_username, body(1,0), "")
                        pinfo.cols.info:append(' username=""')
                    end

                else
                    resp_tree:add_expert_info(
                        PI_MALFORMED, PI_ERROR,
                        "Unexpected option tag: " .. tostring(tag)
                    )
                end
            end

        else
            resp_tree:add_expert_info(
                PI_UNDECODED, PI_WARN,
                "Unknown response type: " .. tostring(kind)
            )
        end
    end
end

------------------------------------------------------------
-- Top-level dissector with manual TCP reassembly
------------------------------------------------------------

function aspenrs.dissector(tvbuf, pinfo, tree)
    local total_len = tvbuf:len()
    local offset    = 0

    while offset < total_len do
        local remaining = total_len - offset

        -- Need at least header bytes
        if remaining < MSG_HDR_LEN then
            pinfo.desegment_len    = MSG_HDR_LEN - remaining
            pinfo.desegment_offset = offset
            return
        end

        local hdr_buf = tvbuf(offset, remaining)

        -- MessageHeader.kind
        local kind = hdr_buf(0,1):uint()

        -- MessageHeader.payload_len: u64 big-endian
        local len_field = hdr_buf(1, LEN_LENGTH):uint64()
        local payload_len = len_field:tonumber()

        local full_len = MSG_HDR_LEN + payload_len

        if remaining < full_len then
            -- ask TCP for more bytes
            pinfo.desegment_len    = full_len - remaining
            pinfo.desegment_offset = offset
            return
        end

        -- We have a full PDU
        local pdu_buf = tvbuf(offset, full_len)
        dissect_message(pdu_buf, pinfo, tree, kind, payload_len)

        offset = offset + full_len
    end
end

------------------------------------------------------------
-- Registration
------------------------------------------------------------

local tcp_table = DissectorTable.get("tcp.port")
tcp_table:add(SERVER_PORT, aspenrs)
