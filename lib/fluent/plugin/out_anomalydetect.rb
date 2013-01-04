# -*- coding: utf-8 -*-
module Fluent
  class AnomalyDetectOutput < Output
    Fluent::Plugin.register_output('anomalydetect', self)
    
    require 'fluent/plugin/change_finder'

    config_param :outlier_term, :integer, :default => 28
    config_param :outlier_discount, :float, :default => 0.05
    config_param :smooth_term, :integer, :default => 3
    config_param :score_term, :integer, :default => 28
    config_param :score_discount, :float, :default => 0.05
    config_param :tick, :integer, :default => 60 * 5
    config_param :tag, :string, :default => "anomaly"
    config_param :value, :string

    attr_accessor :outlier
    attr_accessor :score

    attr_accessor :outliers
    attr_accessor :scores

    attr_accessor :records

    def configure (conf)
      super
      @outliers = []
      @scores = []
      @outlier  = ChangeFinder.new(@outlier_term, @outlier_discount)
      @score    = ChangeFinder.new(@score_term, @score_discount)

      @records = {}
      @mutex = Mutex.new
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
      
      output = {}
      flushed.each do |tick, records|
        output[tick] = records.inject(0.0) do |sum, record| sum += record[@value] if record[@value]; end / records.size
      end

      output.each do |tick, val|
        outlier = @outlier.next(val)
        @outliers.push outlier
        @outliers.shift() if @outliers.size > @smooth_term
        score = @score.next(@outliers.inject(0) do |sum, v| sum += v end / @outliers.size)

        output["outlier"] = outlier
        output["score"] = score
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
