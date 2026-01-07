#!/bin/bash

# 启动 Streamlit (前端监控)
# --server.port 指定为 Zeabur 要求的端口 (默认 8080)
# --server.headless=true 无头模式
# --server.address=0.0.0.0 允许外部访问
echo "🚀 Starting Streamlit on port $PORT..."
nohup streamlit run deployment_hf/hf_monitor.py --server.port $PORT --server.headless=true --server.address=0.0.0.0 > system.log 2>&1 &

# 启动 实盘策略 (后端核心)
# 使用 python -u 确保日志实时输出
echo "🚀 Starting Shannon Strategy..."
python -u 策略仓库/八号香农策略/real_trading.py
