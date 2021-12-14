# /home/davidm/local/src/hpguppi_daq/src/meerkat_init.sh
using Blio.GuppiRaw
using Hashpipe
using ArgParse
using Statistics
using Hashpipe: update_status, status_t, check_databuf, HASHPIPE_OK, HASHPIPE_TIMEOUT
using HPGuppi: hpguppi_databuf_t

# Custom hit channel databuf
include("hit_save_thread.jl")

include("/home/mhawkins/local/src/Search.jl")
using .Search: SearchThread
include("/home/mhawkins/local/src/SpectralKurtosis.jl")
#include("search_thread.jl")
#import ..Search_Thread: SearchThread

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
			default = 2
		"--start_input_block"
			help = "hashpipe input block to start processing on"
			arg_type = Int
			default = 4
		"--output_db"
			help = "output hashpipe databuf to store hits in"
			arg_type = Int
			default = 3
		"--start_output_block"
			help = "hashpipe output block to start processing on"
			arg_type = Int
			default = 0
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

function init(instance_id, input_db_id, output_db_id, searchAlgoName, searchAlgo,
              skey="SRHSTAT",input_block_id=21, output_block_id=0, n_input_blocks=24, n_output_blocks=24)

    # Check that hashpipe status exists
    if Hashpipe.status_exists(instance_id) == 0
        @error "Hashpipe instance $(instance_id) does not exist."
    end

    st = status_t(0,0,0,0)
	Hashpipe.status_attach(instance_id, Ref(st))

    Hashpipe.status_buf_lock_unlock(Ref(st)) do
        Hashpipe.update_status(st,  "SRHSTAT", "Initing"); # TODO: figure out why putting skey doesn't work
    end

    # TODO: Check for databuf existence first
	input_db_p = Hashpipe.databuf_attach(instance_id, input_db_id)
    # Input databuf
	if input_db_id >= 0
		if input_db_p == C_NULL
			@error "Input databuf $(input_db_id) doesn't exist - This should be created by an upstream Hashpipe thread."
		end
		# Populate hpguppi_db with values of selected databuf
		input_db = hpguppi_databuf_t(input_db_p)
	else
		input_db, input_db_p = Ptr{Cvoid}
	end

    # Output databuf
	if output_db_id >= 0
		# if check_databuf(instance_id, output_block_id) == nothing
		# 	@info "Output databuf doesn't exist - Creating output databuf..."

		# 	output_db_block_size = Int(HPGuppi.BLOCK_HDR_SIZE + BLOCK_CHAN_SIZE) # Block data size ivided by number of channels since only storing at max 1 channel

		# 	output_db_p = Hashpipe.databuf_create(instance_id, output_block_id, HPGuppi.BLOCK_HDR_SIZE, output_db_block_size, n_output_blocks)
			# p_db = Hashpipe.databuf_create(0, 99, 4096, 8593408, 40)
			# Output:
			# 	Tue Mar 30 05:31:23 2021 : Info (databuf_create): total_size 343740416
			# 	Tue Mar 30 05:31:23 2021 : Info (databuf_create): total_size 1GB aligned 1073741824 (40000000)
			# 	Tue Mar 30 05:31:23 2021 : Info (databuf_create): total_size 2MB aligned 343932928 (14800000)
			# 	Tue Mar 30 05:31:23 2021 : Info (databuf_create): created shared memory for key 8034e474 without huge pages
			# 	Data Type: unknown
			# 	Header Size: 4096
			# 	Num Blocks: 40
			# 	Block Size: 8593408
			# 	shmid: 4947971
			# 	semid: 458754

		output_db_p = Hashpipe.databuf_attach(instance_id, output_db_id)
		if output_db_p == C_NULL
			@info "Output databuf doesn't exist - Creating output databuf..."

			output_db_p = Hashpipe.databuf_create(instance_id, output_db_id, HPGuppi.BLOCK_HDR_SIZE, HITS_INFO_BLOCK_SIZE , HPGuppi.N_INPUT_BLOCKS)
		else

		end
		# Populate hpguppi_db with values of selected databuf
		output_db = hits_info_databuf_t(output_db_p)

	else
		output_db, output_db_p = Ptr{Cvoid}
	end

    search_thread = SearchThread(skey, instance_id, searchAlgoName, st,
								input_db_p, output_db_p, input_db, output_db,
								input_block_id, output_block_id, n_input_blocks, n_output_blocks)

    return search_thread
end
"""

Write GUPPI RAW header data and hit channels to output databuf
"""
function write_hit_data(thread, plan, grh, raw_block)
	@info "Writing hit data to output thread"
	@info "GRH: $grh"
	# Development testing writing of hit info:
	log_filename = "test_log1.txt"
	f=open(log_filename, "w")
	write(f, plan.sk_hits_info)
	close(f)

	# Open HDF5 hits file
	hits_fid = h5open("test_log.hdf5", "cw")

	# Write GUPPI RAW header data and channel data for each hit to a databuf output block
	for hit_chan in findall(x->x[1]==1, hits_info[:,1])

		# Busy loop to wait for Free block
		while (rv=Hashpipe.databuf_wait_free(thread.output_db_p, thread.output_block_id)) != Hashpipe.HASHPIPE_OK
			if rv==HASHPIPE_TIMEOUT
				@warn "Search thread ($(thread.searchAlgoName)) write timeout waiting for free block"
			else
				@error "Search thread ($(thread.searchAlgoName)) writer error waiting for free databuf - Error: $rv"
			end
		end
		# Add hit metadata to output GUPPI header
		grh["HITCHAN"] = hit_chan
		grh["SK_PIZA"] = hits_info[hit_chan, 2] # Spectral Kurtosis pizazz



	end

	close(hits_fid)
end

function run(thread::SearchThread, sk_plan)
	time_per_block = 0

    # Lock status buffer before updating key/value pairs
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "SRHSTAT", "Waiting");
        Hashpipe.update_status(thread.status, "SRHBLKIN", thread.input_block_id);
        Hashpipe.update_status(thread.status, "SRHBKOUT", thread.output_block_id);

    end

    # Busy loop to wait for filled block
    while (rv=Hashpipe.databuf_wait_filled(thread.input_db_p, thread.input_block_id)) != Hashpipe.HASHPIPE_OK
        if rv==HASHPIPE_TIMEOUT
            @warn "Search thread ($(thread.searchAlgoName)) timeout waiting for filled block"
        else
            @error "Search thread ($(thread.searchAlgoName)) error waiting for filled databuf - Error: $rv"
        end
    end

	tick = time_ns()

    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "SRHSTAT", "Processing");
        #Hashpipe.update_status(thread.status, "SRHBLKMS", string(block_proc_time));
    end


    # Calculation on block data
	# TODO: Too slow currently: 252ms
    SpectralKurtosis.exec_plan(sk_plan, thread.input_db.blocks[thread.input_block_id].p_data)
	tock = time_ns()
    # Return metadata for each hit
    SpectralKurtosis.hit_mask_chan(sk_plan)

	# if(sum(sk_plan.sk_hits_info[:,1]) > 0) # If hits are found
	# 	grh, raw_block = HPGuppi.hpguppi_get_hdr_block(thread.input_db.blocks[thread.input_block_id])
	# 	#write_hit_data(thread, sk_plan, grh, raw_block)
	# end

	# Copy GUPPI RAW header to output databuf
	# Takes 79us for MeerKAT test data
	unsafe_copyto!(thread.output_db.blocks[thread.output_block_id + 1].p_hdr, thread.input_db.blocks[thread.input_block_id + 1].p_hdr, HPGuppi.BLOCK_HDR_SIZE)
	# Copy hits info data to output databuf current output block data pointer
	# This operation takes 14 microseconds for 16x2 Float32
	unsafe_copyto!(thread.output_db.blocks[thread.output_block_id + 1].p_data, pointer(sk_plan.sk_hits_info), length(sk_plan.sk_hits_info))
	# Mark output databuf filled
	Hashpipe.databuf_set_filled(thread.output_db_p, thread.input_block_id)

    # Free block after all calculations
	# Undo for operation!!!
    # Hashpipe.hashpipe_databuf_set_free(thread.input_db_p, thread.input_block_id)

    # Increment through input blocks cycle
    thread.input_block_id  = (thread.input_block_id  + 1) % thread.n_input_blocks
    thread.output_block_id = (thread.output_block_id + 1) % thread.n_output_blocks


	# All non-processing work takes ~0.03ms to run
	# => Negligible overhead. Timing times the plan execution and masking
	time_per_block = time_per_block = Int(tock - tick) / 1e6
	@info "Elapsed/Block (ms): $time_per_block"

    return nothing
end

"""
	main()

The primary calculation thread for hashpipe pipeline.
"""
function main()
	args = parse_commandline() # Parse command-line args

	instance_id     = args["inst_id"] # Hashpipe instance to attach to
	input_db_id     = args["input_db"] # Hashpipe databuf to read data from
	input_block_id  = args["start_input_block"] # Starting input block to process
	output_db_id    = args["output_db"] # Hashpipe databuf to read data from
	output_block_id = args["start_output_block"] # Starting input block to process
	verbose         = args["verbose"]


	@info "Creating spectral kurtosis plan..."
	sk_plan = SpectralKurtosis.create_sk_plan(Complex{Int8}, (2, 32768, 16, 64), [256,2048], 0.003)

	@info "Instantiating search thread..."
	search_th = init(instance_id, input_db_id, output_db_id, "Spectral Kurtosis", sk_plan)

	# while true
	@info "Running search thread..."
	run(search_th, sk_plan)

	@info "--- $(Int(sum(sk_plan.sk_hits_info[:,1]))) Hits found! ---"

	return sk_plan
	# end
end

#main()
