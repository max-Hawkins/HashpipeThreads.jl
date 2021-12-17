# A Hashpipe thread that correlates multi-antenna raw voltage streams.
# Currently using John Romein's Tensor Core Correlator codebase.
# TODO: Link

using Hashpipe
using ArgParse
using TCC

mutable struct CorrelatorTCCThread <: Hashpipe.HashpipeThread
    instance_id::Int
    skey::String
    status::Hashpipe.status_t

    input_db_p::Union{Ptr{Hashpipe.databuf_t}, Ptr{Cvoid}}
    input_db::Union{HPGuppi.hpguppi_databuf_t, Ptr{Cvoid}}
    input_db_id::Int
    input_block_id::Int
    n_input_blocks::Int

    output_db_p::Union{Ptr{Hashpipe.databuf_t}, Ptr{Cvoid}}
    output_db::Union{Hashpipe.databuf_t, Ptr{Cvoid}}
    output_db_id::Int
    output_block_id::Int
    n_output_blocks::Int

    config::TCC.config
    voltages_gpu::CuArray
    samples_gpu::CuArray
    visibilities_gpu::CuArray

    running::Bool
end

# TODO: Alter easy constructor or delete if not necessary
# Easy constructor
function CorrelatorTCCThread(instance_id, input_db_id, input_block_id, output_db_id, output_block_id)

    return CorrelatorTCCThread(instance_id, 
                                "CORSTAT", 
                                Hashpipe.status_t(0,0,0,0), 

                                Ptr{Cvoid}(C_NULL), 
                                Ptr{Cvoid}(C_NULL), 
                                input_db_id,
                                input_block_id,
                                24,

                                true)

end

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
		"--input_db"
			help = "input hashpipe databuf containing voltage data"
			arg_type = Int
			default = 1
		"--start_input_block"
			help = "hashpipe input block to start processing on"
			arg_type = Int
			default = 1
        "--output_db"
			help = "output hashpipe databuf to store visibilties in"
			arg_type = Int
		"--start_output_block"
			help = "hashpipe output block to start processing on"
			arg_type = Int
			default = 1
        "--tcc_ptx_file"
			help = "TCC PTX file containing CUDA correlate function"
			arg_type = String
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
    init(thread::CorrelatorTCCThread)

Create and setup a Hashpipe thread to correlate voltage data using John Romein's CUDA Tensor Core Correlator library.
"""
function init_thread(thread::CorrelatorTCCThread)
    println("Initing Hashpipe Correlator TCC thread")

    # Check that hashpipe status exists
    if Hashpipe.status_exists(thread.instance_id) == 0
        @error "Hashpipe instance $(thread.instance_id) does not exist."
    end

    # Attach to Hashpipe status and update to initializing
	Hashpipe.status_attach(thread.instance_id, Ref(thread.status))
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, thread.skey, "Initing");
    end

    # Attach to input databuf
    if thread.input_db_id > 0
        thread.input_db_p = Hashpipe.databuf_attach(thread.instance_id, thread.input_db_id)
		if thread.input_db_p == C_NULL
			@error "input databuf doesn't exist. Must create input databuf before initiating this thread."
            thread.running = false
            return
		end
		# Populate hpguppi_db with values of selected databuf
		thread.input_db = hpguppi_databuf_t(thread.output_db_p)
	else
        @error "input databuf ID is less than 1. Databuf ID's should be greater than 0."
        return
	end

    # Attach to output databuf
    if thread.output_db_id > 0
        thread.output_db_p = Hashpipe.databuf_attach(thread.instance_id, thread.output_db_id)
		if thread.output_db_p == C_NULL
			@warn "Output databuf doesn't exist. Creating..."
            # TODO: Create output databuf
		end
		# Populate hpguppi_db with values of selected databuf
		thread.output_db = hpguppi_databuf_t(thread.output_db_p)
	else
        @error "output databuf ID is less than 1. Databuf ID's should be greater than 0."
        return
	end

end

"""
    run(thread::CorrelatorTCCThread)

Run a Hashpipe thread that correlates voltage data using the tensor core correlator developed John Romein.
"""
function run_thread(thread::CorrelatorTCCThread)
    
    @info "Processing block: $(thread.input_block_id)    Outputting to block: $(thread.output_block_id)"

    # Update hashpipe status to waiting
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "CORRIN", thread.input_block_id);
        Hashpipe.update_status(thread.status, "CORROUT", thread.output_block_id);
    end

    # Busy loop to wait for filled block (note converstion from 1-indexed to 0-indexed block_id)
    while (rv=Hashpipe.databuf_wait_filled(thread.input_db_p, thread.input_block_id - 1)) != Hashpipe.HASHPIPE_OK
        if rv==Hashpipe.HASHPIPE_TIMEOUT
            @warn "Correlator TCC thread timeout waiting for filled input block" maxlog=1
        else
            @error "Correlator TCC thread error waiting for free databuf - Error: $rv"
        end
    end

    # Mark input block filled and increment
    Hashpipe.databuf_set_free(thread.input_db_p, thread.input_block_id - 1)
    thread.input_block_id = (thread.input_block_id % thread.n_input_blocks) + 1

    # Correlator operation
    # TODO:
    # Unsafe wrap pointer to current block's data section as voltage array
    # Run TCC.raw_to_tcc()
    # Run TCC.correlate()

    # Busy loop to wait for free output block (note converstion from 1-indexed to 0-indexed block_id)
    while (rv=Hashpipe.databuf_wait_free(thread.output_db_p, thread.output_block_id - 1)) != Hashpipe.HASHPIPE_OK
        if rv==Hashpipe.HASHPIPE_TIMEOUT
            @warn "Correlator TCC thread timeout waiting for free block" maxlog=1
        else
            @error "Correlator TCC thread error waiting for free output block - Error: $rv"
        end
    end

    # Mark input block filled and increment
    Hashpipe.databuf_set_filled(thread.output_db_p, thread.output_block_id - 1)
    thread.output_block_id = (thread.output_block_id % thread.n_output_blocks) + 1
end

"""
	main()

The primary thread for Hashpipe thread initialization and execution in a pipeline.
"""
function main()
	args = parse_commandline() # Parse command-line args

    # TODO: Alter command line arguments to your needs. Two examples given.
    instance_id     = args["inst_id"] # Hashpipe instance to attach to
	input_db_id     = args["input_db"] # Hashpipe databuf to read voltage data from
	input_block_id  = args["start_input_block"] # Starting input block to process
    output_db_id     = args["output_db"] # Hashpipe databuf to write visibility data to
	output_block_id  = args["start_output_block"] # Starting output block to process
    tcc_ptx_file     = args["tcc_ptx_file"] # TCC-compiled PTX file containing CUDA correlate function

    @info "Setting up TCC Correlator Thread"
    thread = CorrelatorTCCThread(instance_id)
    init_thread(thread)

    @info "Running TCC Correlator Thread"
    while(thread.running)
        run_thread(thread)
    end

end

main()
