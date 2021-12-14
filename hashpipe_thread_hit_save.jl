using Blio.GuppiRaw, Hashpipe, ArgParse, HDF5
using Hashpipe: update_status, status_t, HashpipeThread, HashpipeDatabuf, HASHPIPE_OK, HASHPIPE_TIMEOUT

# hpguppi functionality (databufs mainly)
using HPGuppi: hpguppi_databuf_t

# Custom hit channel databuf
include("hit_save_thread.jl")

mutable struct HitSaveThread <: HashpipeThread
    skey::String
    instance_id::Int
    searchAlgoName::String
    status::status_t
    input_db_p::Union{Ptr{databuf_t}, Ptr{Cvoid}}
    input_db::Union{HashpipeDatabuf, Ptr{Cvoid}}
    input_block_id::Int
    n_input_blocks::Int
	hit_filename::String
	hit_file::HDF5.File
end

"""
Parse commandline arguments for hashpipe calculation thread.
"""
function parse_commandline()
	s = ArgParseSettings()

	@add_arg_table s begin
		"--inst_id"
			help = "hashpipe instance id to attach to"
			arg_type = Int
			default = 0
		"--input_db"
			help = "input hashpipe databuf to get raw data from"
			arg_type = Int
			default = 3
		"--start_input_block"
			help = "hashpipe input block to start processing on"
			arg_type = Int
			default = 0
		"--hit_filename"
			help = "HDF5 file to store hit info in "
			arg_type = String
			default = "test_hits.hdf5"
		"--verbose"
			help = "verbose output"
			action = :store_true
	end

	args = parse_args(s)
	println(args)
	if args["verbose"]
		println("Parsed args:")
		for (arg,val) in args
			println(" $arg => $val")
		end
	end
	return args
end

function init(instance_id, input_db_id, input_block_id, hit_filename, skey="HITSTAT",)

    # Check that hashpipe status exists
    if Hashpipe.status_exists(instance_id) == 0
        @error "Hashpipe instance $(instance_id) does not exist."
    end

    st = status_t(0,0,0,0)
	Hashpipe.status_attach(instance_id, Ref(st))

    Hashpipe.status_buf_lock_unlock(Ref(st)) do
        Hashpipe.update_status(st,  "HITSTAT", "Initing"); # TODO: figure out why putting sk doesn't work
    end

    # TODO: Check for databuf existence first

    # Input databuf
	if input_db_id >= 0
		input_db_p = Hashpipe.databuf_attach(instance_id, input_db_id)
		if input_db_p == C_NULL # If couldn't attach to databuf
			@error "Input databuf doesn't exist - It should be created as an output databuf of the search thread."
		end

		# Populate hpguppi_db with values of selected databuf
		input_db = hits_info_databuf_t(input_db_p)
	else
		@error "Invalid input databuf specified. ID must be >=0"
	end
	# Open hit HDF5 file (delete in case of existing file)
	hit_file = h5open(hit_filename, "w")
	# Create HDF5 groups - mimicing uvh5 format
	create_group(hit_file, "Header")
	create_group(hit_file, "Data")


	return HitSaveThread(skey, instance_id, "Spectral Kurtosis",
				st, input_db_p, input_db, input_block_id, HPGuppi.N_INPUT_BLOCKS, hit_filename, hit_file)
end

function run(thread::HitSaveThread)
	@info "Running HitSaveThread..."


	# Each block is a group under the top-level data group with time-sensitive parameters
	# Each channel is then a dataset under the block group with a pizazz parameters
	# The Header top-level group contains time-insensitive GUPPI RAW header records

end


"""
	main()

The primary calculation thread for hashpipe pipeline.
"""
function main()
	args = parse_commandline() # Parse command-line args

	instance_id   = args["inst_id"] # Hashpipe instance to attach to
	input_db_id   = args["input_db"] # Hashpipe databuf to read data from
	input_block_id  = args["start_input_block"] # Starting input block to process
	hit_filename  = args["hit_filename"]
	verbose       = args["verbose"]

	hit_save_thread = init(instance_id, input_db_id, input_block_id, hit_filename)

    run(hit_save_thread)

	return nothing
	# end
end
