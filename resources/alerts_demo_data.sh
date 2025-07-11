#!/usr/bin/env bash

# Configuration
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}

# Pre-compute auth header
AUTH_HEADER="Authorization: Basic $(echo -n "$P_USERNAME:$P_PASSWORD" | base64)"

# Common curl function with retry logic
curl_with_retry() {
    local url="$1"
    local method="$2"
    local data="$3"
    local content_type="${4:-application/json}"
    local max_retries="${5:-3}"
    local retry_count=0
    local data_file="$7"
    
    # Create temp file if data is provided (either as string or file)
    if [[ -n "$data_file" ]]; then
        temp_file="$data_file"
    elif [[ -n "$data" ]]; then
        temp_file=$(mktemp)
        if [[ $? -ne 0 ]]; then
            return 1
        fi
        printf "%s" "$data" > "$temp_file"
    fi
    
    while [[ $retry_count -lt $max_retries ]]; do
        local max_time=$((10 + (retry_count * 10)))
        local connect_timeout=5
        
        local curl_cmd="curl -s -w \"\n%{http_code}\" --max-time $max_time --connect-timeout $connect_timeout"
        
        # Add headers
        curl_cmd+=" -H \"Content-Type: $content_type\""
        curl_cmd+=" -H \"$AUTH_HEADER\""
        
        # Add method and data
        if [[ "$method" == "POST" ]]; then
            curl_cmd+=" -X POST"
            if [[ -n "$temp_file" ]]; then
                curl_cmd+=" --data-binary \"@$temp_file\""
            elif [[ -n "$data" ]]; then
                curl_cmd+=" -d \"$data\""
            fi
        fi
        
        # Add URL
        curl_cmd+=" \"$url\""
        
        # Execute curl
        local response
        response=$(eval "$curl_cmd" 2>&1)
        local curl_exit_code=$?
        
        # Check curl exit code
        if [[ $curl_exit_code -eq 0 ]]; then
            local status_code
            if [[ -n "$response" ]]; then
                status_code=$(echo "$response" | tail -n1)
                local response_body=$(echo "$response" | sed '$d')
                
                # Clean up temp file (only if we created it)
                if [[ -n "$temp_file" && -z "$data_file" ]]; then
                    rm -f "$temp_file"
                fi
                
                if [[ "$status_code" == "200" || "$status_code" == "201" ]]; then
                    echo "$response_body"
                    return 0
                else
                    return 1
                fi
            else
                return 1
            fi
        elif [[ $curl_exit_code -eq 28 ]]; then
            # Timeout - retry
            retry_count=$((retry_count + 1))
            sleep 1
        else
            break
        fi
    done
    
    # Clean up temp file on failure (only if we created it)
    if [[ -n "$temp_file" && -z "$data_file" ]]; then
        rm -f "$temp_file"
    fi
    
    return 1
}

# Create webhook target
create_target() {
    response=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST "$P_URL/api/v1/targets" -d @- << EOF
{"type":"webhook","endpoint":"https://webhook.site/8e1f26bd-2f5b-47a2-9d0b-3b3dabb30710","name":"Test Webhook","auth":{"username":"","password":""},"skipTlsCheck":false,"notificationConfig":{"interval":1,"times":1}}
EOF
)
    
    curl_exit_code=$?
    
    if [[ $curl_exit_code -eq 0 && -n "$response" ]]; then
        # Extract target ID from response
        target_id=$(echo "$response" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
        if [[ -n "$target_id" ]]; then
            echo "Target created successfully with ID: $target_id" >&2
            echo "$target_id"
            return 0
        else
            echo "Failed to extract target ID from response" >&2
            echo "Response: $response" >&2
            return 1
        fi
    else
        echo "Failed to create target" >&2
        echo "Curl exit code: $curl_exit_code" >&2
        echo "Response: $response" >&2
        return 1
    fi
}

# Create JSON file to avoid control character issues
create_json_file() {
    local filename="$1"
    local content="$2"
    
    # Create temporary file with proper JSON content
    temp_json=$(mktemp)
    printf "%s" "$content" > "$temp_json"
    echo "$temp_json"
}

# Create alerts
create_alerts() {
    local target_id="$1"
    
    if [[ -z "$target_id" ]]; then
        echo "Target ID is required to create alerts"
        return 1
    fi
    
    # Alert 1: Error Count (severity_number = 18)
    alert1_template='{"severity":"high","title":"error count","stream":"STREAM_PLACEHOLDER","alertType":"threshold","aggregates":{"aggregateConfig":[{"aggregateFunction":"count","conditions":{"operator":null,"conditionConfig":[{"column":"severity_number","operator":"=","value":"18"}]},"column":"severity_number","operator":">","value":1000}]},"evalConfig":{"rollingWindow":{"evalStart":"5h","evalEnd":"now","evalFrequency":1}},"targets":["TARGET_PLACEHOLDER"]}'
    
    alert1_json=$(echo "$alert1_template" | sed "s|STREAM_PLACEHOLDER|$P_STREAM|g" | sed "s|TARGET_PLACEHOLDER|$target_id|g")
    response1=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST -d "$alert1_json" "$P_URL/api/v1/alerts")
    if [[ $? -eq 0 && -n "$response1" ]]; then
        echo "Alert 1 created successfully"
    else
        echo "Failed to create Alert 1"
        echo "Response: $response1"
    fi
    
    # Alert 2: 400 Errors
    
    alert2_template='{"severity":"critical","title":"400 Errors","stream":"STREAM_PLACEHOLDER","alertType":"threshold","aggregates":{"aggregateConfig":[{"aggregateFunction":"count","conditions":{"operator":null,"conditionConfig":[{"column":"body","operator":"contains","value":"400"}]},"column":"body","operator":">","value":10}]},"evalConfig":{"rollingWindow":{"evalStart":"5h","evalEnd":"now","evalFrequency":1}},"targets":["TARGET_PLACEHOLDER"]}'
    
    alert2_json=$(echo "$alert2_template" | sed "s|STREAM_PLACEHOLDER|$P_STREAM|g" | sed "s|TARGET_PLACEHOLDER|$target_id|g")
    
    response2=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST -d "$alert2_json" "$P_URL/api/v1/alerts")
    if [[ $? -eq 0 && -n "$response2" ]]; then
        echo "Alert 2 created successfully"
    else
        echo "Failed to create Alert 2"
        echo "Response: $response2"
    fi
    
    # Alert 3: Trace ID or Span ID null
    
    alert3_template='{"severity":"high","title":"Trace ID or Span ID null","stream":"STREAM_PLACEHOLDER","alertType":"threshold","aggregates":{"aggregateConfig":[{"aggregateFunction":"count","conditions":{"operator":null,"conditionConfig":[{"column":"trace_id","operator":"is null","value":""}]},"column":"trace_id","operator":">","value":0}]},"evalConfig":{"rollingWindow":{"evalStart":"5h","evalEnd":"now","evalFrequency":1}},"targets":["TARGET_PLACEHOLDER"]}'
    
    alert3_json=$(echo "$alert3_template" | sed "s|STREAM_PLACEHOLDER|$P_STREAM|g" | sed "s|TARGET_PLACEHOLDER|$target_id|g")
    
    response3=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST -d "$alert3_json" "$P_URL/api/v1/alerts")
    if [[ $? -eq 0 && -n "$response3" ]]; then
        echo "Alert 3 created successfully"
    else
        echo "Failed to create Alert 3"
        echo "Response: $response3"
    fi
    
    sleep 1
}

# Main execution
# Create target and get ID
target_id=$(create_target)

if [[ $? -eq 0 && -n "$target_id" ]]; then
    echo "Target creation successful, proceeding with alerts..."
    sleep 2
    
    # Create alerts using the target ID
    create_alerts "$target_id"
else
    echo "Failed to create target, cannot proceed with alerts"
    exit 1
fi

exit 0