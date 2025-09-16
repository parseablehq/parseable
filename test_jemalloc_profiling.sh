#!/bin/bash

echo "🚀 Starting jemalloc profiling test for Parseable..."

# Set jemalloc profiling configuration
export MALLOC_CONF="prof:true,prof_active:true,prof_prefix:parseable.prof,prof_leak:true,prof_interval:1048576"

echo "📊 MALLOC_CONF: $MALLOC_CONF"
echo ""

# Start Parseable with profiling in background
echo "🔧 Starting Parseable with profiling enabled..."
./target/release/parseable s3-store &
PARSEABLE_PID=$!

echo "📋 Parseable PID: $PARSEABLE_PID"
echo "⏱️  Letting it run for 30 seconds to generate profile data..."

# Let it run for a bit to generate some profile data
sleep 30

# Stop Parseable
echo "🛑 Stopping Parseable..."
kill $PARSEABLE_PID
wait $PARSEABLE_PID 2>/dev/null

echo ""
echo "📁 Generated profile files:"
ls -la parseable.prof* 2>/dev/null || echo "No profile files found"

echo ""
echo "✅ Profiling test complete!"
echo ""
echo "🔍 To analyze profiles, use:"
echo "   jeprof --text ./target/release/parseable parseable.prof.*.heap"
echo "   jeprof --svg ./target/release/parseable parseable.prof.*.heap > memory.svg"
