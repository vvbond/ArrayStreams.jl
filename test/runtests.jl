using ArrayStreams
using Test
using LinearAlgebra

@testset "ArrayStreams.jl" begin
  @testset "CircularBufferArray: scalar" begin
    @testset "Buffer underflow" begin
      cb = CircularBufferArray{Int, 0}(10)
      @test length(cb) == 0
      @test cb.count == 0
      @test cb.width == 10
      @test cb.elsize == ()
      @test cb.ix == collect(1:10)
      @test cb.data == zeros(Int, 10)
      @test cb[] == zeros(Int, 10)
      @test cb[:] == [] 

      push!(cb, 1)
      @test length(cb) == 1
      @test cb.count == 1
      @test cb[:] == [1]
      @test cb[] == [zeros(Int,9)..., 1] 

      push!(cb, 2:5)
      @test length(cb) == 5
      @test cb.count == 5
      @test cb[:] == [1, 2, 3, 4, 5]
      @test cb[] == vcat(zeros(Int,5), [1,2,3,4,5]) 

      push!(cb, 6:10)
      @test length(cb) == 10
      @test cb.count == 10
      @test cb[:] == collect(1:10)
      @test cb[] == collect(1:10)
    end
    @testset "Buffer overflow" begin
      cb = CircularBufferArray{Int, 0}(10)
      push!(cb, 1:10)
      @test length(cb) == 10
      @test cb.count == 10
      @test cb[:] == collect(1:10)
      @test cb[] == collect(1:10)

      push!(cb, 11)
      @test length(cb) == 10
      @test cb.count == 11
      @test cb[:] == collect(2:11)
      @test cb[] == collect(2:11)

      push!(cb, 12:15)
      @test length(cb) == 10
      @test cb.count == 15
      @test cb[:] == collect(6:15)
      @test cb[] == collect(6:15)

      push!(cb, 16:20)
      @test length(cb) == 10
      @test cb.count == 20
      @test cb[:] == collect(11:20)
      @test cb[] == collect(11:20)
    end
  end
  @testset "CircularBufferArray: 1D" begin
    @testset "Buffer underflow" begin
      cb = CircularBufferArray{Int, 1}(10)
      @test ismissing(cb.elsize )
      push!(cb, ones(Int, 5))
      @test cb.elsize == (5,)
      B = ones(Int, 5, 1)
      @test cb[:] == B
      @test cb[] == hcat(zeros(Int, 5, 9), B)

      push!(cb, ones(Int, 5)*2)
      B = ones(Int, 5, 2)*diagm([1, 2])
      @test cb[:] == B
      @test cb[] == hcat(zeros(Int, 5, 8), B)
    end
    @testset "Buffer overflow" begin
      cb = CircularBufferArray{Int, 1}(10)
      B = ones(Int, 3, 9)*diagm(1:9)
      push!(cb, B)
      @test cb.elsize == (3,)
      @test length(cb) == 9
      @test cb.count == 9
      @test cb[:] == B
      @test cb[] == hcat(zeros(Int, 3, 1), B)

      B = ones(Int, 3, 7)*diagm(10:16)
      push!(cb, B)
      C = ones(Int, 3, 10)*diagm(7:16)
      @test length(cb) == 10 
      @test cb.count == 16
      @test cb[:] == C
      @test cb[] == C 
    end
  end
  @testset "CircularBufferArray Iterator" begin
    cb = CircularBufferArray{Int, 1}(10)
    B = ones(Int, 4, 10)*diagm(1:10)
    push!(cb, B)
    cbiter = CircularBufferArrayIterator(cb, (width = 6, hop = 3))
    @test length(cbiter) == 2
    @test first(cbiter) == cb[][:, 1:6]
    @test length(cbiter) == 1
    @test first(cbiter) == cb[][:, (1:6).+3]
    @test length(cbiter) == 0

    B = ones(Int, 4, 5)*diagm(11:15)
    push!(cb, B)
    @test length(cbiter) == 2
    @test first(cbiter) == ones(Int, 4, 6)*diagm(7:12)
    @test first(cbiter) == ones(Int, 4, 6)*diagm(10:15)
  end
  @testset "ArrayStream" begin
     In = Channel{Vector{Int}}(ch->foreach(i->put!(ch, ones(Int,3)*i), 1:100), 10)
     window = (width=5, hop=2)
     Out = ArrayStream(In, window; size = 10)
     ix = 1
     for d in Out
       @test d == ones(Int, 3, window.width)*diagm(ix:ix+window.width-1)
       ix += window.hop
     end
  end
end
