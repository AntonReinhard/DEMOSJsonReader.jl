using EzXML

const COMPUTE_NODE = 1
const DATA_NODE = 0

function _read_graphml_header(root::EzXML.Node, attr_name_type::String, attr_name_tasktype::String)
    type_attr_str = ""
    tasktype_attr_str = ""

    graph_elem = nothing
    for child in eachelement(root)
        if child.name == "key"
            if child["for"] == "node" && child["attr.name"] == attr_name_type
                type_attr_str = child["id"]
            elseif child["for"] == "node" && child["attr.name"] == attr_name_tasktype
                tasktype_attr_str = child["id"]
            end
        end
        # the main node that contains the graph structure
        if child.name == "graph"
            graph_elem = child
            break
        end
    end

    if graph_elem === nothing
        error("GraphML file $filename: no <graph> element found under root")
    end
    if type_attr_str == ""
        error("GraphML file $filename: no <key> element with the attr.name \"$attr_name_type\" found")
    end
    if tasktype_attr_str == ""
        error("GraphML file $filename: no <key> element with the attr.name \"$attr_name_tasktype\" found")
    end

    return graph_elem, type_attr_str, tasktype_attr_str
end

function _node_type(node::EzXML.Node, type_attr_str::String, type_compute_identifier::String, type_data_identifier::String)
    for d in eachelement(node)
        if !haskey(d, "key")
            # @warn?
            continue
        end
        if (d["key"] == type_attr_str && d.content == type_compute_identifier)
            return COMPUTE_NODE
        elseif (d["key"] == type_attr_str && d.content == type_data_identifier)
            return DATA_NODE
        elseif (d["key"] == type_attr_str)
            error("found node (id=$(node["id"]) without a valid type; found \"$(d.content)\", expected $type_compute_identifier or $type_data_identifier")
        end
    end
    error("found node (id=$(node["id"]) without a valid type")
    # unreachable
    return
end

function _task_type(node::EzXML.Node, tasktype_attr_str::String)
    for d in eachelement(node)
        if !haskey(d, "key")
            continue
        end
        if (d["key"] == tasktype_attr_str)
            return d.content
        end
    end
    error("found compute node (id=$(node["id"]) without a valid task type, no key with $(tasktype_attr_str) was found")
    # unreachable
    return
end


"""
    read_graphml(filename::String; kwargs)

Parse a .graphml file and return a [`DAG`](@ref).

## kwargs:
- `attr_name_type`: The `attr.name` value for differentiating the type (compute vs data) of nodes
- `type_compute_identifier`: The expected value of the `attr_name_type` attribute for compute nodes.
- `type_data_identifier`: The expected value of the `attr_name_type` attribute for data nodes.
- `attr_name_tasktype`: The `attr.name` value for compute node's ComputeTask names.
"""
function read_graphml(
        filename::String,
        context_module::Module;
        attr_name_type::String = "type",
        attr_name_tasktype::String = "node_id",
        type_compute_identifier::String = "Algorithm",
        type_data_identifier::String = "DataObject",
    )
    doc = readxml(filename)
    root = EzXML.root(doc)

    graph_elem, type_attr_str, tasktype_attr_str = _read_graphml_header(root, attr_name_type, attr_name_tasktype)

    # save the nodes to insert edges later
    nodes = Dict{String, ComputableDAGs.Node}()

    # Construct the DAG
    cdag = DAG()

    # Iterate over all nodes
    for node_elem in eachelement(graph_elem)
        if (node_elem.name == "node")
            node_type = _node_type(node_elem, type_attr_str, type_compute_identifier, type_data_identifier)

            if node_type == COMPUTE_NODE
                task_type = _task_type(node_elem, tasktype_attr_str)
                task_type = _sanitize_name(task_type) * "()"
                n = insert_node!(cdag, context_module.eval(Meta.parse(task_type)))
                nodes[node_elem["id"]] = n
            elseif node_type == DATA_NODE
                n = insert_node!(cdag, DataTask(0))
                nodes[node_elem["id"]] = n
            end
        elseif node_elem.name == "edge"
            from = node_elem["source"]
            to = node_elem["target"]
            insert_edge!(cdag, nodes[from], nodes[to])
        end
    end

    # add data input node
    input_node = insert_node!(cdag, DataTask(0))
    entry_nodes = get_entry_nodes(cdag)

    for n in entry_nodes
        if (n isa DataTaskNode)
            continue
        end
        insert_edge!(cdag, input_node, n)
    end

    return cdag
end

function compute_tasks_graphml(
        filename::String;
        attr_name_type::String = "type",
        attr_name_tasktype::String = "node_id",
        type_compute_identifier::String = "Algorithm",
        type_data_identifier::String = "DataObject",
    )
    task_names = Set{String}()

    doc = readxml(filename)
    root = EzXML.root(doc)

    graph_elem, type_attr_str, tasktype_attr_str = _read_graphml_header(root, attr_name_type, attr_name_tasktype)

    # Iterate over all nodes
    for node_elem in eachelement(graph_elem)
        if (node_elem.name != "node")
            continue
        end

        node_type = _node_type(node_elem, type_attr_str, type_compute_identifier, type_data_identifier)
        if node_type != COMPUTE_NODE
            continue
        end

        push!(task_names, _task_type(node_elem, tasktype_attr_str))
    end

    return task_names
end
