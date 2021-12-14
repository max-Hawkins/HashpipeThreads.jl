using Blio
using Hashpipe
using ArgParse
using HPGuppi
using Glob

mutable struct GuppiRawHashpipeThread <: Hashpipe.HashpipeThread
    filename::String # TODO: Change to make generalizable for entire observations and folders
    instance_id::Int
    skey::String
    cur_file::Union{IO, Nothing}
    status::Hashpipe.status_t
    output_db_p::Union{Ptr{Hashpipe.databuf_t}, Ptr{Cvoid}}
    output_db::Union{HPGuppi.hpguppi_databuf_t, Ptr{Cvoid}}
    output_db_id::Int
    output_block_id::Int
    n_output_blocks::Int
    header::GuppiRaw.Header
    files_to_proc::Vector{String}
    running::Bool
end

# Easy constructor
function GuppiRawHashpipeThread(filename, instance_id, output_db_id)
    GuppiRawHashpipeThread(filename, 
                           instance_id, 
                           "RAWSTAT", 
                           nothing, 
                           Hashpipe.status_t(0,0,0,0),
                           Ptr{Cvoid}(C_NULL), 
                           Ptr{Cvoid}(C_NULL), 
                           output_db_id, 
                           1, 
                           24,
                           GuppiRaw.Header(), 
                           Vector{String}(),
                           true)

end

"""
Parse commandline arguments for hashpipe calculation thread.
"""
function parse_commandline()
	s = ArgParseSettings()

	@add_arg_table s begin
		"--guppi_filename"
			help = "Guppi RAW file to process"
			arg_type = String
        "--inst_id"
			help = "hashpipe instance id to attach to"
			arg_type = Int
			default = 0
		"--output_db"
			help = "output hashpipe databuf to store hits in"
			arg_type = Int
			default = 1
		"--start_output_block"
			help = "hashpipe output block to start processing on"
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
    Find new files to process (if any) and set running status to false if no more files to process
"""
function find_files(thread::GuppiRawHashpipeThread)
    # If given filename is a single RAW file
    if endswith(thread.filename, ".raw")
        push!(thread.files_to_proc, thread.filename)

    # If filename is a folder
    elseif isdir(thread.filename)
        thread.files_to_proc = glob("*.raw", thread.filename)
    
    # If the filename is a stem of RAW files
    else
        last_delim_idx = findlast('/', thread.filename)
        folder = thread.filename[1 : last_delim_idx]
        stem = thread.filename[last_delim_idx + 1 : end] * "*.raw"
        thread.files_to_proc = glob(stem, folder)
    end    

    @info "$(size(thread.files_to_proc, 1)) Guppi RAW files to read:"
    for file in thread.files_to_proc
        println(file)
    end
    return nothing
end

"""
    Close the currently open file if it's open, check to see if there are more files to process,
    and if so, open the next file to process.
"""
function open_next_file(thread)
    # Close currently opened file 
    if thread.cur_file != nothing
        close(thread.cur_file)
    end

    # Stop running if no more files to process
    if thread.files_to_proc == String[]
        @info "No more files to process. Stopping..."
        thread.running = false
        return nothing
    end

    # Open next file to process and remove it from files to process (even though we haven't finished processing yet)
    try
        file = popfirst!(thread.files_to_proc)
        thread.cur_file = open(file)
    catch error
        @error "Error opening file: $(file)"
        throw(error)
    end
    @info "Processing $file"
    return nothing
end

"""
    init(thread::GuppiRawHashpipeThread)

Create and setup a hashpipe thread to read in GUPPI RAW data. Later will be put into HPGuppi.jl.
"""
function init_thread(thread::GuppiRawHashpipeThread)
    println("Initing Hashpipe GUPPI RAW reader input thread with filename reader: $thread.filename")
    
    find_files(thread)
    
    open_next_file(thread)

    # Check that hashpipe status exists
    if Hashpipe.status_exists(thread.instance_id) == 0
        @error "Hashpipe instance $(thread.instance_id) does not exist."
    end

    # Attach to Hashpipe status and update to initializing
	Hashpipe.status_attach(thread.instance_id, Ref(thread.status))
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, thread.skey, "Initing");
    end

    if thread.output_db_id > 0
		thread.output_db_p = Hashpipe.databuf_attach(thread.instance_id, thread.output_db_id)
		if thread.output_db_p == C_NULL
			@info "Output databuf doesn't exist - Creating output databuf..."
            # Create HPGuppi databuf
			thread.output_db_p = Hashpipe.databuf_create(thread.instance_id, thread.output_db_id,
                                     HPGuppi.BLOCK_HDR_SIZE, HPGuppi.BLOCK_SIZE, HPGuppi.N_INPUT_BLOCKS)
		end
		# Populate hpguppi_db with values of selected databuf
		thread.output_db = hpguppi_databuf_t(thread.output_db_p)
	else
        @error "Output databuf ID is less than 1. Databuf ID's should be greater than 0."
	end

end

"""
    run(thread::GuppiRawHashpipeThread)

Run a Guppi Raw reader thread.
Later will be put into HPGuppi.jl.
"""
function run_thread(thread::GuppiRawHashpipeThread)
    # TODO: Check that current file is valid, and if at EOF, figure out which .raw filename to open next

    # If done reading current file, close file and stop thread
    if eof(thread.cur_file)
        @info "Reached end of file for $(thread.filename). Closing."
        open_next_file(thread)
        if !thread.running
            return nothing
        end
    end
    @info "Processing block: $(thread.output_block_id)"

    # Update hashpipe status to waiting
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "RAWSTAT", "Waiting");
        Hashpipe.update_status(thread.status, "RAWBOUT", thread.output_block_id);
    end

    # Busy loop to wait for free block (note converstion from 1-indexed to 0-indexed block_id)
    while (rv=Hashpipe.databuf_wait_free(thread.output_db_p, thread.output_block_id - 1)) != Hashpipe.HASHPIPE_OK
        if rv==Hashpipe.HASHPIPE_TIMEOUT
            @warn "Guppi Raw thread timeout waiting for free block" maxlog=1
        else
            @error "Guppi Raw thread error waiting for free databuf - Error: $rv"
        end
    end

    # Update status buffer to processing
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "RAWSTAT", "Procing");
    end

    # Copy GUPPI RAW block header and data to Hashpipe databuffer
    try
        # Mark start of block header
        mark(thread.cur_file)
        # Read Guppi RAW header with Blio
        read!(thread.cur_file, thread.header)
        # Calculate number of bytes in header (including DirectIO padding)
        hdr_nbytes = position(thread.cur_file) - reset(thread.cur_file);
        # Read header data into databuffer block's header region
        unsafe_read(thread.cur_file, thread.output_db.blocks[thread.output_block_id].p_hdr, hdr_nbytes)
        # Read voltage data into databuffer block's data region
        unsafe_read(thread.cur_file, thread.output_db.blocks[thread.output_block_id].p_data, thread.header.blocsize)
    catch
        @error "Error reading from current file. Stopping pipeline."
        close(thread.cur_file)
        thread.running = false
        return nothing
    end
    # TODO: Make these type of calls 1-indexed
    # Mark block filled
    Hashpipe.databuf_set_filled(thread.output_db_p, thread.output_block_id - 1)

    # Increment output_block_id
    thread.output_block_id = (thread.output_block_id % thread.n_output_blocks) + 1

    # TODO: Is this necessary?
    # Update status buffer to done
    Hashpipe.status_buf_lock_unlock(Ref(thread.status)) do
        Hashpipe.update_status(thread.status, "RAWSTAT", "Done");
    end
end

"""
	main()

The primary thread for Hashpipe thread initialization and execution in a pipeline.
"""
function main()
	args = parse_commandline() # Parse command-line args

	guppi_filename  = args["guppi_filename"] # "/home/mhawkins/guppi_56520_VOYAGER1_0012.0000.raw"
    instance_id     = args["inst_id"] # Hashpipe instance to attach to
	output_db_id    = args["output_db"] # Hashpipe databuf to read data from
	output_block_id = args["start_output_block"] # Starting input block to process
	verbose         = args["verbose"]

    @info "Setting up Guppi Raw reader thread"
    raw_thread = GuppiRawHashpipeThread(guppi_filename, instance_id, output_db_id)
    init_thread(raw_thread)

    @info "Running Guppi Raw reader thread"
    while(raw_thread.running)
        run_thread(raw_thread)
    end

    @info "Cleaning up Guppi RAW reader thread resources"
    # TODO: Free shared memory
end

main()
