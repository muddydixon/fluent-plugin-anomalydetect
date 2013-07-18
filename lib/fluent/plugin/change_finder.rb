# -*- coding: utf-8 -*-
module Fluent
  class ChangeFinder
    require 'matrix'

    def initialize(term, r)
      @term = term
      @r = r
      @data = []
      @mu = 0
      @sigma = 0
      @c = (0..@term - 1).map { |i| rand }
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
      w = (Matrix.rows(cc).inv * Vector.elements(@c)).to_a
      xt = @data.each.with_index.inject(@mu) do |sum, (v, idx)|
        sum += w[idx] * (v - @mu)
      end
      @sigma = (1 - @r) * @sigma + @r * (x - xt) * (x - xt)

      @data.push x
      if @data.size > @term
        @data.shift
      end

      score(prob xt, @sigma, x)
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
