# A template Hashpipe thread for copying and editing to create custom threads.

using Hashpipe
using ArgParse
# TODO: Change thread name and struct contents. Some example fields given
mutable struct YourCustomThread <: Hashpipe.HashpipeThread
    instance_id::Int
    skey::String
    status::Hashpipe.status_t
    # input_db_p::Union{Ptr{Hashpipe.databuf_t}, Ptr{Cvoid}}
    # input_db::Union{Hashpipe.databuf_t, Ptr{Cvoid}}
    # input_db_id::Int
    # input_block_id::Int
    # n_input_blocks::Int
    running::Bool
end

# TODO: Alter easy constructor or delete if not necessary
# Easy constructor
YourCustomThread(instance_id) =
YourCustomThread(instance_id, "STAT", Hashpipe.status_t(0,0,0,0), true)

"""
Parse commandline arguments for hashpipe calculation thread.
"""
function parse_commandline()
	s = ArgParseSettings()
    # TODO: Alter command line arguments
	@add_arg_table s begin
        "--inst_id"
			help = "hashpipe instance id to attach to"
			arg_type = Int
			default = 0
		# "--input_db"
		# 	help = "input hashpipe databuf to store hits in"
		# 	arg_type = Int
		# 	default = 1
		# "--start_input_block"
		# 	help = "hashpipe input block to start processing on"
		# 	arg_type = Int
		# 	default = 1
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

"""
    init(thread::YourCustomThread)

Create and setup a dummy Hashpipe thread to just mark filled blocks as free.
"""
function init_thread(thread::YourCustomThread)
    println("Initing Hashpipe TODO: Insert name here thread.")

    # Check that hashpipe status exists
    if Hashpipe.status_exists(thread.instance_id) == 0
        @error "Hashpipe instance $(thread.instance_id) does not exist."
    end

    # Attach to Hashpipe status and update to initializing
	Hashpipe.status_attach(thread.instance_id, Ref(thread.status))
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, thread.skey, "Initing");
    end

    if thread.input_db_id > 0
        # thread.output_db_p = Hashpipe.databuf_attach(thread.instance_id, thread.output_db_id)
		# if thread.output_db_p == C_NULL
		# 	@info "Output databuf doesn't exist - Creating output databuf..."
        #     # Create HPGuppi databuf
		# 	thread.output_db_p = Hashpipe.databuf_create(thread.instance_id, thread.output_db_id,
        #                              HPGuppi.BLOCK_HDR_SIZE, HPGuppi.BLOCK_SIZE, HPGuppi.N_INPUT_BLOCKS)
		# end
		# # Populate hpguppi_db with values of selected databuf
		# thread.output_db = hpguppi_databuf_t(thread.output_db_p)
	else
        @error "input databuf ID is less than 1. Databuf ID's should be greater than 0."
        return
	end

end

"""
    run(thread::YourCustomThread)

Run a dummy Hashpipe thread.
"""
function run_thread(thread::YourCustomThread)
    
    @info "Processing block: $(thread.input_block_id)"

    # Update hashpipe status to waiting
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "TODO: Insert key here", thread.input_block_id);
    end

    # Busy loop to wait for filled block (note converstion from 1-indexed to 0-indexed block_id)
    while (rv=Hashpipe.databuf_wait_filled(thread.input_db_p, thread.input_block_id - 1)) != Hashpipe.HASHPIPE_OK
        if rv==Hashpipe.HASHPIPE_TIMEOUT
            @warn "Dummy thread timeout waiting for free block" maxlog=1
            # TODO: Check if total mask is all zeros? == UInt64(0)
            # If timeout, increment block number to handle when input and output threads are misaligned
            thread.input_block_id = (thread.input_block_id % thread.n_input_blocks) + 1
        else
            @error "Dummy thread error waiting for free databuf - Error: $rv"
        end
    end

    # Mark block filled
    Hashpipe.databuf_set_free(thread.input_db_p, thread.input_block_id - 1)

    # Increment input_block_id
    thread.input_block_id = (thread.input_block_id % thread.n_input_blocks) + 1
end

"""
	main()

The primary thread for Hashpipe thread initialization and execution in a pipeline.
"""
function main()
	args = parse_commandline() # Parse command-line args

    # TODO: Alter command line arguments to your needs. Two examples given.
    instance_id     = args["inst_id"] # Hashpipe instance to attach to
	# input_db_id     = args["input_db"] # Hashpipe databuf to read data from
	# input_block_id  = args["start_input_block"] # Starting input block to process

    @info "Setting up TODO: Insert name here thread"
    thread = YourCustomThread(instance_id)
    init_thread(thread)

    @info "Running TODO: Insert name here thread"
    while(thread.running)
        run_thread(thread)
    end

end

main()
