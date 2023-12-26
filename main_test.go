package main

import (
	"fmt"
	"testing"
)

func TestContains(t *testing.T) {
	type args struct {
		kafkaList []string
		gzfile    string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
		{
			name: "Success",
			args: args{
				kafkaList: []string{
					"{\"Path\":\"DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000400.json.gz\"}",
					"{\"Path\":\"DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000000.json.gz\"}",
				},
				gzfile: "DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000000.json.gz",
			},
			want: true,
		},
		{
			name: "Fail",
			args: args{
				kafkaList: []string{
					"{\"Path\":\"DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000400.json.gz\"}",
					"{\"Path\":\"DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000000.json.gz\"}",
				},
				gzfile: "DataPipeline/F00001/PM-1/ProcessLog/20231201/20231201_000600.json.gz",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		fmt.Printf("%s/n", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			if got := Contains(tt.args.kafkaList, tt.args.gzfile); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}
