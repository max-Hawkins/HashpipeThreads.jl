# A dummy Hashpipe thread that does no operation on Hashpipe buffer data
# but only marks blocks as free

using Hashpipe
using ArgParse

mutable struct DummyThread <: Hashpipe.HashpipeThread
    instance_id::Int
    skey::String
    status::Hashpipe.status_t
    input_db_p::Union{Ptr{Hashpipe.databuf_t}, Ptr{Cvoid}}
    input_db::Union{Hashpipe.databuf_t, Ptr{Cvoid}}
    input_db_id::Int
    input_block_id::Int
    n_input_blocks::Int
    running::Bool
end

# TODO: Parse number of input blocks from input databuf information
# Easy constructor
DummyThread(instance_id, input_db_id) =
DummyThread(instance_id, "DUMSTAT", Hashpipe.status_t(0,0,0,0),
                        Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL), input_db_id, 1, 24, true)

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
			help = "input hashpipe databuf to store hits in"
			arg_type = Int
			default = 1
		"--start_input_block"
			help = "hashpipe input block to start processing on"
			arg_type = Int
			default = 1
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
    init(thread::DummyThread)

Create and setup a dummy Hashpipe thread to just mark filled blocks as free.
"""
function init_thread(thread::DummyThread)
    println("Initing Hashpipe dummy thread.")

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
        # Attach to existing databuffer or wait until it's created (for back->front pipeline creation)
		while (thread.input_db_p = Hashpipe.databuf_attach(thread.instance_id, thread.input_db_id)) == C_NULL
			@warn "Input databuf doesn't exist. Waiting for input databuffer to be created." maxlog=1
        end
	else
        @error "input databuf ID is less than 1. Databuf ID's should be greater than 0."
        return
	end

end

"""
    run(thread::DummyThread)

Run a dummy Hashpipe thread.
"""
function run_thread(thread::DummyThread)
    
    @info "Processing block: $(thread.input_block_id)"

    # Update hashpipe status to waiting
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "DUMBOUT", thread.input_block_id);
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

    instance_id     = args["inst_id"] # Hashpipe instance to attach to
	input_db_id     = args["input_db"] # Hashpipe databuf to read data from
	input_block_id  = args["start_input_block"] # Starting input block to process

    @info "Setting up dummy thread"
    dummy_thread = DummyThread(instance_id, input_db_id)
    init_thread(dummy_thread)

    @info "Running dummy thread"
    while(dummy_thread.running)
        run_thread(dummy_thread)
    end

end

main()
