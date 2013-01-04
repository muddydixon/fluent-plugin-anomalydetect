# -*- coding: utf-8 -*-
module Fluent
  class AnomalyDetectOutput < Output
    Fluent::Plugin.register_output('anomalydetect', self)
    
    require 'fluent/plugin/change_finder'

    config_param :outlier_term, :integer, :default => 28
    config_param :outlier_discount, :float, :default => 0.05
    config_param :smooth_term, :integer, :default => 7
    config_param :score_term, :integer, :default => 14
    config_param :score_discount, :float, :default => 0.1
    config_param :tick, :integer, :default => 60 * 5
    config_param :tag, :string, :default => "anomaly"
    config_param :target, :string, :default => ""

    attr_accessor :outlier
    attr_accessor :score
    attr_accessor :recordCount

    attr_accessor :outliers

    attr_accessor :records

    def configure (conf)
      super
      unless 0 < @outlier_discount and @outlier_discount < 1 
        raise Fluent::ConfigError, "discount ratio should be between (0, 1)" 
      end
      unless 0 < @score_discount and @score_discount < 1 
        raise Fluent::ConfigError, "discount ratio should be between (0, 1)"
      end
      if @outlier_term < 1
        raise Fluent::ConfigError, "outlier term should be greater than 0"
      end
      if @score_term < 1
        raise Fluent::ConfigError, "score term should be greater than 0"
      end
      if @smooth_term < 1
        raise Fluent::ConfigError, "smooth term should be greater than 0"
      end
      if @tick < 10
        raise Fluent::ConfigError, "tick timer should be greater than 10 sec"
      end
      
      @outliers = []
      @outlier  = ChangeFinder.new(@outlier_term, @outlier_discount)
      @score    = ChangeFinder.new(@score_term, @score_discount)

      @records = {}
      @mutex = Mutex.new

      if @target == ""
        @recordCount = true 
      else
        @recordCount = false
      end
    end

    def start
      super
      start_watch
      init_records
    end

    def shutdown
      super
      if @watcher
        @watcher.terminate
        @watcher.join
      end
    end

    def start_watch
      @watcher = Thread.new(&method(:watch))
    end

    def watch
      @last_checked = Fluent::Engine.now
      while true
        sleep 0.5
        if Fluent::Engine.now - @last_checked >= @tick
          now = Fluent::Engine.now
          flush_emit(now - @last_checked)
          @last_checked = now
        end
      end
    end

    def init_records
      @records = {}
    end

    def flush_emit(step)
      output = flush
      Fluent::Engine.emit(@tag, Fluent::Engine.now, output)
    end

    def flush
      flushed, @records = @records, init_records

      _output = {}
      flushed.each do |tick, records|
        if @recordCount
          _output[tick] = records.size
        else
          _output[tick] = records.inject(0.0) do |sum, record| sum += record[@target] if record[@target]; end / records.size
        end
      end
      if _output.keys.size == 0
        tick = tickTime(Fluent::Engine.now)
        if @recordCount
          _output[tick] = 0
        end
      end

      output = []
      _output.each do |tick, val|
        outlier = @outlier.next(val)
        @outliers.push outlier
        @outliers.shift() if @outliers.size > @smooth_term
        score = @score.next(@outliers.inject(0) do |sum, v| sum += v end / @outliers.size)

        output.push({"time" => tick.to_i, "outlier" => outlier, "score" => score, "target" => val})
        # この上の値を出力としてemitする
      end

      output
    end

    def tickTime (time)
      (time - time % @tick).to_s
    end

    def pushRecords (tick, records)
      @mutex.synchronize do
        @records[tick] = [] unless @records[tick]
        @records[tick].concat(records)
      end
    end

    def emit (tag, es, chain)
      times = {}
      es.each do |time, record|
        tick = tickTime(time)
        times[tick] = [] unless times[tick]
        times[tick].push record
      end
      times.each do |tick, records|
        pushRecords tick, records
      end

      chain.next
    end
  end
end
