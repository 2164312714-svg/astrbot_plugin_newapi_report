# New-API 每小时报表 · AstrBot 插件

每小时自动生成 New-API 监控报表，含请求趋势图、模型/用户 Token 排行、服务器状态，发送到 QQ 群。

> 致谢：本插件基于 [huanxherta](https://github.com/huanxherta) 的原始脚本改造而来。

## 功能特性

- 📊 **请求趋势图**：30 分钟一档的柱状图，时间范围可配置（1-48 小时）
- 🏆 **模型 Token 排行**：Top 10 模型消耗排行
- 👤 **用户 Token 排行**：Top 10 用户/IP 消耗排行（同用户不同 IP 分开显示）
- 🖥️ **服务器状态**：CPU、内存、Swap、磁盘、网络、负载、温度、IO、进程等
- ⏰ **定时发送**：每小时自动生成并发送到指定 QQ 群
- 📨 **多群发送**：支持同时发送到多个群
- 🎨 **自定义背景**：支持配置背景图 URL，首次下载后本地缓存
- 🔄 **API 重试**：网络波动自动重试，3 次递增退避

## 安装

1. 在 AstrBot 插件市场搜索 `newapi_report` 或手动安装：
   ```
   https://github.com/2164312714-svg/astrbot_plugin_newapi_report
   ```
2. 安装依赖（Docker 环境需额外操作，见下方说明）

## 配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `newapi_url` | New-API 地址 | 无 |
| `newapi_token` | 系统访问令牌 | 无 |
| `newapi_user_id` | 管理员用户 ID | 无 |
| `group_ids` | 发送群号（多群逗号分隔） | 无 |
| `cron_minute` | 每小时第几分钟执行 | 5 |
| `enable_cron` | 启用定时报表 | true |
| `time_range_hours` | 报表时间范围（小时） | 9 |
| `browser_executable` | Chromium 路径（留空自动下载） | 空 |
| `background_url` | 背景图 URL（可选） | 空 |

### 获取系统访问令牌

1. 登录 New-API 管理后台
2. 个人设置 → 安全设置 → 系统访问令牌
3. 生成令牌并填入配置

> **注意**：需要管理员账号生成令牌，同时需填写管理员用户 ID（通常为 1）。

### IP Token 排行

IP 排行功能需在 New-API 后台「个人设置 → Other Settings → IP Logging」中手动开启，默认关闭。

## 使用

- **手动触发**：在群内发送 `/报表`
- **定时发送**：配置 `enable_cron=true` 后，每小时第 N 分钟自动发送

## Docker 环境注意事项

插件依赖 Playwright 无头浏览器截图，Docker 容器内需安装系统依赖：

```bash
# 进入 AstrBot 容器
docker exec -it astrbot bash

# 安装 Playwright + Chromium
pip install playwright
npx playwright install chromium
npx playwright install-deps chromium
```

也可配置 `browser_executable` 指向容器内已有的 Chromium：
```
/usr/bin/chromium
```

## 示例截图

![报表示例](screenshot.jpg)

## License

[MIT](LICENSE)
