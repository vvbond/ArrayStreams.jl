module ArrayStreams

import Base: iterate, push!
export CircularBufferArray, CircularBufferArrayIterator, ArrayStream

const Window = @NamedTuple{length::Int, stride::Int}

#%% CircularBufferArray
mutable struct CircularBufferArray{T, N}
  width::Int
  elsize::Union{Missing, NTuple{N, Int}}
  data::Union{Missing, Array{T}}
  ix::Vector{Int}
  count::Int
end

function CircularBufferArray{T,N}(width::Int, elsize::Union{Missing, Tuple{Int}} = missing) where {T, N}
  data = ismissing(elsize) ? missing : 
         T <: Number ? zeros(T, elsize..., width) : 
         Array{T, N+1}(undef, elsize..., width)
  ix = collect(1:width)
  count = 0
  CircularBufferArray{T,N}(width, elsize, data, ix, count)
end

function CircularBufferArray{T,0}(width::Int) where {T}
  data = T <: Number ? zeros(T, width) : Array{T, 1}(undef, width)
  ix = collect(1:width)
  count = 0
  CircularBufferArray{T,0}(width, (), data, ix, count)
end

function init!(cb::CircularBufferArray{T,N}, elsize::NTuple{N,Int}) where {T,N}
  cb.elsize = elsize
  cb.data = T <: Number ? zeros(T, elsize..., cb.width) : 
            Array{T, N+1}(undef, elsize..., cb.width)
  nothing
end

function push!(cb::CircularBufferArray{T,N}, data::Array{T,K}) where {T,N,K}
  sz = size(data)
  ismissing(cb.elsize) && init!(cb, sz[1:N])
  n = K == N && all(sz == cb.elsize) ? 1 : 
      K == N+1 && all(sz[1:end-1] == cb.elsize) ? sz[end] : 
      nothing

  @assert !isnothing(n) "Dimension error."
  @assert n <= cb.width "Data block exceeds buffer width."

  cb.data[ntuple(_->(:), N)..., cb.ix[1:n]] = data
  _update_count!(cb, n)
  cb
end

function push!(cb::CircularBufferArray{T,0}, data::T) where T
  cb.data[cb.ix[1]] = data
  _update_count!(cb, 1)
  cb
end

function push!(cb::CircularBufferArray{T,0}, data::Array{T,1}) where T
  n = length(data)
  @assert n <= cb.width "Data block exceeds buffer width."
  cb.data[cb.ix[1:n]] = data
  _update_count!(cb, n)
  cb
end
push!(cb::CircularBufferArray{T, 0}, data::UnitRange{T}) where T = push!(cb, collect(data))

function _update_count!(cb::CircularBufferArray, n)
  cb.count += n
  cb.ix[:] = circshift(cb.ix, -n)
end

data(cb::CircularBufferArray{T,N}) where {T,N} = cb.data[ntuple(_->(:), N)..., cb.ix]

Base.length(cb::CircularBufferArray{T,N}) where {T,N} = min(cb.count, cb.width)
Base.IndexStyle(::CircularBufferArray) = IndexLinear()
Base.getindex(cb::CircularBufferArray{T,N}, i::Union{Int, UnitRange{Int}}) where {T,N} = cb.data[ntuple(_->(:),N)..., cb.count > cb.width ? cb.ix[i] : i]
Base.getindex(cb::CircularBufferArray, ::Colon) = cb[1:end]
Base.getindex(cb::CircularBufferArray{T,N}) where {T,N} = cb.data[ntuple(_->(:),N)..., cb.ix]
Base.lastindex(cb::CircularBufferArray) = min(cb.count, cb.width)

# Base.show(io::IO, cb::CircularBufferArray) = Base.show(io::IO, cb[:])

#%% Iterator interface
mutable struct CircularBufferArrayIterator
  cb::CircularBufferArray
  window::@NamedTuple{length::Int, stride::Int}
  count::Int
  dim::Int
end

function CircularBufferArrayIterator(cb::CircularBufferArray, window::Window)
  @assert window.length <= cb.width "Window width can't exceed the width of the buffer."
  count = max(0, cb.count - cb.width)
  dim = length(cb.elsize)
  CircularBufferArrayIterator(cb, window, count, dim)
end

function Base.iterate(iter::CircularBufferArrayIterator, state = nothing) 
  iter.count + iter.window.length > iter.cb.count && return nothing
  ix = wrap!(iter.count .+ (1:iter.window.length), iter.cb.width)
  D = iter.cb.data[ntuple(_->(:), iter.dim)..., ix]
  iter.count += iter.window.stride
  (D, count)
end

Base.length(iter::CircularBufferArrayIterator) = fld(min(length(iter.cb), iter.cb.count-iter.count)-iter.window.length + iter.window.stride, iter.window.stride)

wrap!(v::Vector{Int}, n::Int) = mod.(v.-1, n) .+ 1
wrap!(v::UnitRange{Int}, n::Int) = wrap!(collect(v), n)

#%% ArrayStream
function stream(src::AbstractChannel, sink::AbstractChannel, window::Window, NDims::Int=1, f=identity)
  d = take!(src) |> f
  n = 1
  if isa(d, Number) || isa(d, AbstractArray{<:Number})
      n = ndims(d) == NDims ? 1 : ndims(d) == NDims + 1 ? size(d, NDims + 1) : error("Inconsistent dimensions.")
  elseif NDims != 0
      warning("Non-numeric types are treated as scalars: setting NDims=0.")
      NDims = 0
  end
  T = NDims == 0 ? typeof(d) : eltype(d)
  width = max(3n, window.length + n)
  buffer = CircularBufferArray{T,NDims}(width)
  push!(buffer, d)
  bufferitr = CircularBufferArrayIterator(buffer, window)
  for data in bufferitr
    put!(sink, data)
  end
  for d in src
    push!(buffer, f(d))
    for data in bufferitr
      put!(sink, data)
    end
  end
end
ArrayStream(src::AbstractChannel, window::Window, NDims::Int=1; size::Int=1000, f = identity, taskref=nothing) = Channel((sink)->stream(src, sink, window, NDims, f), size; taskref)

end
