#using .Hashpipe: databuf_t, HashpipeDatabuf
#using .HPGuppi: N_INPUT_BLOCKS, PADDING_SIZE, BLOCK_HDR_SIZE

const BLOCK_CHAN_SIZE = Int(HPGuppi.BLOCK_DATA_SIZE / 16)

# """
# hpguppi_chan_databuf_t
# """
# struct hpguppi_chan_databuf_t <: HashpipeDatabuf
# p_hp_db::Ptr{databuf_t}
# blocks::Array{HPGuppi.hpguppi_block_t}

# function hpguppi_chan_databuf_t(p_hp_db::Ptr{databuf_t})
#     blocks_array = Array{HPGuppi.hpguppi_block_t}(undef, N_INPUT_BLOCKS)
#     p_blocks = p_hp_db + sizeof(databuf_t) + PADDING_SIZE
#     for i = 0:N_INPUT_BLOCKS - 1
#         p_header = p_blocks + i * BLOCK_CHAN_SIZE
#         p_data = p_header + BLOCK_HDR_SIZE
#         blocks_array[i+1] = HPGuppi.hpguppi_block_t(p_header, p_data)
#     end
#     new(p_hp_db, blocks_array)
# end

# end # struct


const HITS_INFO_ARRAY_SIZE = (16,2) # (Channel, Pizazz)
const HITS_INFO_ARRAY_TYPE = Float32
const HITS_INFO_BLOCK_DATA_SIZE = sizeof(HITS_INFO_ARRAY_TYPE) * HITS_INFO_ARRAY_SIZE[1] * HITS_INFO_ARRAY_SIZE[2]
const HITS_INFO_BLOCK_SIZE = BLOCK_HDR_SIZE + HITS_INFO_BLOCK_DATA_SIZE

struct hits_info_block_t
    p_hdr::Ptr{UInt8}
    p_data::Ptr{HITS_INFO_ARRAY_TYPE}
end

struct hits_info_databuf_t <: HashpipeDatabuf
    p_hp_db::Ptr{databuf_t}
    blocks::Array{hits_info_block_t}

function hits_info_databuf_t(p_hp_db::Ptr{databuf_t})
    blocks_array = Array{hits_info_block_t}(undef, N_INPUT_BLOCKS)
    p_blocks = p_hp_db + sizeof(databuf_t) + PADDING_SIZE
    for i = 0:N_INPUT_BLOCKS - 1
        p_header = p_blocks + i * HITS_INFO_BLOCK_DATA_SIZE
        p_data = p_header + BLOCK_HDR_SIZE
        blocks_array[i+1] = hits_info_block_t(p_header, p_data)
    end
    new(p_hp_db, blocks_array)
end

end # struct
