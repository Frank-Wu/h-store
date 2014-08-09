#pragma once
#ifndef __TIME_MEASURER_H__
#define __TIME_MEASURER_H__

#include <chrono>
using std::chrono::high_resolution_clock;
using std::chrono::system_clock;
using std::chrono::milliseconds;

class TimeMeasurer{
public:
	TimeMeasurer(){}
	virtual ~TimeMeasurer(){}

	void StartTimer(){
		start_time_ = high_resolution_clock::now();
	}
	void EndTimer(){
		end_time_ = high_resolution_clock::now();
	}
	long long GetElapsedMilliSeconds(){
		return std::chrono::duration_cast<milliseconds>(end_time_ - start_time_).count();
	}

private:
	TimeMeasurer(const TimeMeasurer&);
	TimeMeasurer& operator=(const TimeMeasurer&);

private:
	system_clock::time_point start_time_;
	system_clock::time_point end_time_;
};

#endif