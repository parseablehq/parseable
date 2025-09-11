#!/bin/bash

echo "🚀 Comprehensive jemalloc profiling demonstration"
echo ""

# Clean up any old profiles
rm -f parseable.prof* test.prof* 2>/dev/null

echo "📋 Step 1: Building Parseable with jemalloc profiling support..."
cargo build --features jemalloc-prof --release

echo ""
echo "📊 Step 2: Setting up profiling environment..."

# Configure jemalloc for profiling
export MALLOC_CONF="prof:true,prof_active:true,prof_prefix:parseable.prof,prof_interval:4194304,prof_leak:true"
export P_ADDR="0.0.0.0:8200"  # Use unique port
export P_USERNAME="admin"
export P_PASSWORD="admin"

echo "   MALLOC_CONF: $MALLOC_CONF"
echo "   Server will run on port 8200"
echo ""

echo "🔧 Step 3: Starting Parseable with profiling (will run for 30 seconds)..."
echo "   This will generate memory profile data during normal operation"

# Start Parseable in background
./target/release/parseable s3-store &
PARSEABLE_PID=$!

echo "   Parseable PID: $PARSEABLE_PID"
echo "   Letting it initialize and run for 30 seconds..."

# Wait for it to start up
sleep 5

echo "   Making some HTTP requests to generate memory activity..."
# Make some requests to trigger memory allocations
for i in {1..5}; do
    curl -s -u admin:admin http://localhost:8200/api/v1/logstream > /dev/null 2>&1 || true
    sleep 1
done

# Let it run a bit more
sleep 20

echo ""
echo "🛑 Step 4: Stopping Parseable and checking for profile files..."
kill $PARSEABLE_PID 2>/dev/null || true
wait $PARSEABLE_PID 2>/dev/null || true

sleep 2

echo ""
echo "📁 Generated profile files:"
if ls parseable.prof* 1> /dev/null 2>&1; then
    ls -la parseable.prof*
    echo ""
    
    PROFILE_FILE=$(ls parseable.prof*.heap 2>/dev/null | head -1)
    if [ -n "$PROFILE_FILE" ]; then
        echo "🔍 Step 5: Analyzing profile data..."
        echo ""
        echo "=== MEMORY PROFILE ANALYSIS ==="
        echo ""
        echo "Top memory consumers:"
        jeprof --text ./target/release/parseable "$PROFILE_FILE" 2>/dev/null | head -20 || echo "jeprof analysis failed"
        
        echo ""
        echo "📊 To generate visual graph:"
        echo "   jeprof --svg ./target/release/parseable $PROFILE_FILE > memory_profile.svg"
        echo ""
        echo "🌐 To view in web browser:"
        echo "   jeprof --web ./target/release/parseable $PROFILE_FILE"
        echo ""
        echo "✅ Profile analysis complete! File: $PROFILE_FILE"
    else
        echo "❌ No .heap profile files found"
    fi
else
    echo "❌ No profile files generated"
    echo ""
    echo "💡 This can happen if:"
    echo "   - Not enough memory was allocated to trigger profiling interval"
    echo "   - Application didn't run long enough"
    echo "   - Profile interval was too high"
    echo ""
    echo "🔧 Try with lower interval:"
    echo "   MALLOC_CONF=\"prof:true,prof_active:true,prof_prefix:parseable.prof,prof_interval:1048576\""
fi

echo ""
echo "✅ Jemalloc profiling demonstration complete!"
