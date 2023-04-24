package backoff

import (
	"reflect"
	"testing"
	"time"
)

func TestRandExpBackoff_WithMinBackoff(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	type args struct {
		minBackoff time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RandExpBackoff
	}{
		{
			name: "change_millisecond_to_second",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
			args: args{
				minBackoff: time.Second,
			},
			want: &RandExpBackoff{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Second,
			},
		},
		{
			name: "change_second_to_millisecond",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Second,
			},
			args: args{
				minBackoff: time.Millisecond,
			},
			want: &RandExpBackoff{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			if got := bo.WithMinBackoff(tt.args.minBackoff); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.WithMinBackoff() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandExpBackoff_WithStartAttempt(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	type args struct {
		attempt int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RandExpBackoff
	}{
		{
			name: "set_to_10",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
			args: args{
				attempt: 10,
			},
			want: &RandExpBackoff{
				attempt:     10,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			if got := bo.WithStartAttempt(tt.args.attempt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.WithStartAttmpt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandExpBackoff_WithMaxAttempts(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	type args struct {
		maxAttempts int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RandExpBackoff
	}{
		{
			name: "set_to_10",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
			args: args{
				maxAttempts: 10,
			},
			want: &RandExpBackoff{
				attempt:     0,
				limit:       0,
				maxAttempts: 10,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			if got := bo.WithMaxAttempts(tt.args.maxAttempts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.WithMaxAttempts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandExpBackoff_WithScale(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	type args struct {
		scaleFator float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *RandExpBackoff
	}{
		{
			name: "set_to_2.0",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
			args: args{
				scaleFator: 2.5,
			},
			want: &RandExpBackoff{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       2.5,
				minBackoff:  time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			if got := bo.WithScale(tt.args.scaleFator); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.WithScale() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandExpBackoff_SleepAndIncr(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		want    *RandExpBackoff
		wantErr bool
	}{
		{
			name: "require_some_delay",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       20.0,
				minBackoff:  time.Millisecond,
			},
			want: &RandExpBackoff{
				attempt:     1,
				limit:       0,
				maxAttempts: 0,
				scale:       20.0,
				minBackoff:  time.Millisecond,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			tStart := time.Now()
			got, err := bo.SleepAndIncr()
			delay := time.Since(tStart)
			if (err != nil) != tt.wantErr {
				t.Errorf("RandExpBackoff.SleepAndIncr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.SleepAndIncr() = %v, want %v", got, tt.want)
			}

			expectedDelay := tt.fields.minBackoff
			if delay < expectedDelay {
				t.Errorf("RandExpBackoff.SleepAndIncr(), delay too small, expected %v > %v", delay, expectedDelay)
				return
			}
		})
	}
}

func TestRandExpBackoff_getOrCreateWithDefaults(t *testing.T) {
	type fields struct {
		attempt     int
		limit       int
		maxAttempts int
		scale       float64
		minBackoff  time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   *RandExpBackoff
	}{
		{
			name: "require_some_delay",
			fields: fields{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
			want: &RandExpBackoff{
				attempt:     0,
				limit:       0,
				maxAttempts: 0,
				scale:       1.0,
				minBackoff:  time.Millisecond,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bo := &RandExpBackoff{
				attempt:     tt.fields.attempt,
				limit:       tt.fields.limit,
				maxAttempts: tt.fields.maxAttempts,
				scale:       tt.fields.scale,
				minBackoff:  tt.fields.minBackoff,
			}
			if got := bo.getOrCreateWithDefaults(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandExpBackoff.getOrCreateWithDefaults() = %v, want %v", got, tt.want)
			}
		})
	}
}
