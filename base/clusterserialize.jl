# This file is a part of Julia. License is MIT: http://julialang.org/license

type ClusterSerializer{I<:IO} <: AbstractSerializer
    io::I
    counter::Int
    table::ObjectIdDict

    sent_objects::Dict # used by serialize (track objects sent)
    recd_objects::Dict # used by deserialize (object cache)

    ClusterSerializer(io::I) = new(io, 0, ObjectIdDict(), Dict(), Dict())
end
ClusterSerializer(io::IO) = ClusterSerializer{typeof(io)}(io)

function deserialize(s::ClusterSerializer, ::Type{TypeName})
    number, full_body_sent = deserialize(s)
    record_new = nothing
    if !full_body_sent
        if !haskey(s.recd_objects, number)
            error("Expected object in cache. Not found.")
        else
            # println("Located object $number in cache")
            tn = s.recd_objects[number]::TypeName
        end
    else
        name = deserialize(s)
        mod = deserialize(s)
        if haskey(s.recd_objects, number)
            error("Object in cache. Should not have been resent.")
        elseif isdefined(mod, name)
            tn = getfield(mod, name).name
            # TODO: confirm somehow that the types match
            warn(mod, ":",name, " isdefined, need not have been serialized")
            name = tn.name
            mod = tn.module
            makenew = false
            record_new = (t,n)->(s.recd_objects[n]=t)
        else
            name = gensym()
            mod = Serialization.__deserialized_types__
            tn = ccall(:jl_new_typename_in, Ref{TypeName}, (Any, Any), name, mod)
            makenew = true
            record_new = (t,n)->(s.recd_objects[n]=t)
        end
    end
    Serialization.deserialize_cycle(s, tn)
    full_body_sent && Serialization.deserialize_typename_body(s, tn, number, name, mod, makenew, record_new)
    return tn
end

function serialize(s::ClusterSerializer, t::TypeName)
    Serialization.serialize_cycle(s, t) && return
    Serialization.writetag(s.io, Serialization.TYPENAME_TAG)

    identifier = Serialization.object_number(t)
    if t.module === Main && !haskey(s.sent_objects, identifier)
        serialize(s, (identifier, true))
        serialize(s, t.name)
        serialize(s, t.module)
        Serialization.serialize_typename_body(s, t)
        s.sent_objects[identifier] = true
    else
        serialize(s, (identifier, false))
    end
end

# TODO
# Install finalizers on the client side and delete from remote
# Handle wrkr1 -> wrkr2, wrkr1 -> wrkr3 and wrkr2 -> wrkr3
# Handle other modules
