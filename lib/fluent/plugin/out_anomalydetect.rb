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
    config_param :target, :string, :default => nil

    attr_accessor :outlier
    attr_accessor :score
    attr_accessor :record_count

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
      if @tick < 1
        raise Fluent::ConfigError, "tick timer should be greater than 1 sec"
      end
      
      @outliers = []
      @outlier  = ChangeFinder.new(@outlier_term, @outlier_discount)
      @score    = ChangeFinder.new(@score_term, @score_discount)

      @mutex = Mutex.new

      @record_count = @target.nil?
    end

    def start
      super
      init_records
      start_watch
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
      loop {
        sleep 0.5
        now = Fluent::Engine.now
        if now - @last_checked >= @tick
          flush_emit(now - @last_checked)
          @last_checked = now
        end
      }
    end

    def init_records
      @records = []
    end

    def flush_emit(step)
      output = flush
      Fluent::Engine.emit(@tag, Fluent::Engine.now, output)
    end

    def flush
      flushed, @records = @records, init_records

      val = if @record_count
              flushed.size
            else
              flushed.inject(0.0) { |sum, record| sum += record[@target].to_f if record[@target] } / flushed.size
            end

      outlier = @outlier.next(val)
      @outliers.push outlier
      @outliers.shift if @outliers.size > @smooth_term
      score = @score.next(@outliers.inject(0) { |sum, v| sum += v } / @outliers.size)

      {"outlier" => outlier, "score" => score, "target" => val}

    end

    def tick_time(time)
      (time - time % @tick).to_s
    end

    def push_records(records)
      @mutex.synchronize do
        @records.concat(records)
      end
    end

    def emit(tag, es, chain)
      records = es.map { |time, record| record }
      push_records records

      chain.next
    end
  end
end
