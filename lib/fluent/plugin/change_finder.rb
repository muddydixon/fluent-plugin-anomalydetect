# -*- coding: utf-8 -*-
module Fluent
  class ChangeFinder
    require 'matrix'
    require 'ostruct'
    attr_reader :mu
    attr_accessor :log

    def initialize(log, term, r)
      @log = log
      @term = term
      @r = r
      @data = []
      @mu = 0
      @sigma = 0
      @c = (0..@term - 1).map { |i| rand }
    end

    def marshal_dump
      struct = OpenStruct.new
      struct.term = @term
      struct.r = @r
      struct.data = @data
      struct.mu = @mu
      struct.sigma = @sigma
      struct.c = @c
      struct
    end

    def marshal_load(struct)
      @term = struct.term
      @r = struct.r
      @data = struct.data
      @mu = struct.mu
      @sigma = struct.sigma
      @c = struct.c
    end

    def next(x)
      len = @data.size

      # update @mu
      @mu = (1 - @r) * @mu + @r * x

      # update @c
      c = @sigma
      for j in 0..(@term - 1)
        if @data[len - 1 - j]
          @c[j] = (1 - @r) * @c[j] + @r * (x - @mu) * (@data[len - 1 - j] - @mu)
        end
      end

      cc = Matrix.zero(@term).to_a
      for j in 0..(@term - 1)
        for i in j..(@term - 1)
          cc[j][i] = cc[i][j] = @c[i - j]
        end
      end
      
      if Matrix.rows(cc).regular?
        w = (Matrix.rows(cc).inv * Vector.elements(@c)).to_a
      else
        w = (Matrix.rows(cc) * Vector.elements(@c)).to_a
      end
      
      xt = @data.each.with_index.inject(@mu) do |sum, (v, idx)|
        sum += w[idx] * (v - @mu)
      end
      @sigma = (1 - @r) * @sigma + @r * (x - xt) * (x - xt)

      @data.push x
      if @data.size > @term
        @data.shift
      end

      p = prob(xt, @sigma, x)
      s = score(p)
      @log.debug "change_finder:#{Thread.current.object_id} x:#{x} xt:#{xt} p:#{p} s:#{s} term:#{@term} r:#{@r} data:#{@data} mu:#{@mu} sigma:#{@sigma} c:#{@c}"
      s
    end

    def prob(mu, sigma, v)
      return 0 if sigma.zero?

      Math.exp( - 0.5 * (v - mu) ** 2 / sigma) / ((2 * Math::PI) ** 0.5 * sigma ** 0.5)
    end

    def score(p)
      return 0 if p <= 0
      -Math.log(p)
    end

    def smooth(size)
      _end = @data.size
      _begin = [_end - size, 0].max
      (_size = (_end - _begin)) == 0 ? 0.0 : @data.slice(_begin, _end).inject(:+).to_f / _size
    end

    def show_status
      {:sigma => @sigma, :mu => @mu, :data => @data, :c => @c}
    end
  end
end
