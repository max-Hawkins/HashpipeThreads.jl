
export Search_Thread
module Search_Thread

using Hashpipe: hashpipe_status_t, HashpipeThread, HashpipeDatabuf
using .Search: SearchAlgoPlan

mutable struct SearchThread <: HashpipeThread
    skey::String
    instance_id::Cint
    searchAlgoName::String
    cpu_mask::UInt32
    status::hashpipe_status_t
    input_db::HashpipeDatabuf
    output_db::HashpipeDatabuf
    input_block_id::UInt8
    output_block_id::UInt8
    n_input_blocks::UInt8
    n_output_blocks::UInt8
    searchPlan::Search.SearchAlgoPlan
    # TODO: Finish initing all values
    function SearchThread(_skey, _instance_id, _searchAlgoName, _searchAlgo)
        skey = _skey
        instance_id = _instance_id
        searchAlgoName = _searchAlgoName
        cpu_mask = 0
        status = hashpipe_status_t(0,0,0,0)
        searchPlan = Search.SearchAlgoPlan
    end
end

function init(thread::SearchThread)::nothing
    # Check that hashpipe status exists
    if !Hashpipe.hashpipe_status_exists(thread.instance_id)
        @error "Hashpipe instance $(thread.instance_id) does not exist."
    end

    st = hashpipe_status_t(0,0,0,0)
	Hashpipe.hashpipe_status_attach(instance_id, Ref(st))
    thread.status = st

    Hashpipe.hashpipe_status_buf_lock_unlock(Ref(thread.status)) do 
        Hashpipe.update_status(status,  thread.skey, "Initing");
    end

    # TODO: Check for hashpipe databufs and create them if necessary

    init(thread.searchPlan)
    

end

function run(thread::SearchThread)
    # Lock status buffer before updating key/value pairs
    Hashpipe.hashpipe_status_buf_lock_unlock(Ref(thread.status)) do 
        Hashpipe.update_status(status,  thread.skey, "Waiting");
        Hashpipe.update_status(status, "SRHBLKIN", thread.input_block_id);
        Hashpipe.update_status(status, "SRHBKOUT", thread.output_block_id);
            
    end

    # Busy loop to wait for filled block
    while (rv=Hashpipe.hashpipe_databuf_wait_filled(thread.input_db, thread.input_block_id)) != Hashpipe.HASHPIPE_OK
        if rv==HASHPIPE_TIMEOUT
            @warn "Search thread ($(thread.searchAlgoName)) timeout waiting for filled block"
        else
            @error "Search thread ($(thread.searchAlgoName)) error waiting for filled databuf - Error: $rv"
        end 
    end
    
    tick = time_ns()

    Hashpipe.hashpipe_status_buf_lock_unlock(Ref(status)) do
        Hashpipe.update_status(status, "SRHSTAT", "Processing");
        Hashpipe.update_status(status, "SRHBLKMS", string(thread.block_proc_time));
    end

    # Calculation on block data
    exec_plan(thread.searchPlan, thread.input_db.p_data)
    # Return metadata for each hit
    hit_infos = hit_mask(sk_plan)

    # Free block after all calculations
    Hashpipe.hashpipe_databuf_set_free(thread.input_db, thread.input_block_id)

    # Increment through input blocks cycle
    thread.input_block_id = (thread.input_block_id + 1) % thread.n_input_blocks
    #thread.output_block_id = (thread.output_block_id + 1) % thread.n_output_blocks

    tock = time_ns()
    time_per_block = Int(tock - tick) / 1e6
    #println("Elapsed/Block (ms): ", time_per_block) 

    return hit_infos
end

end # module Search_Thread