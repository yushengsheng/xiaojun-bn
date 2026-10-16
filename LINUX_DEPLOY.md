# Linux 傻瓜式安装与升级

适用 Ubuntu 24.04、Debian 12/13，x86_64（AMD64），有 systemd。先通过 SSH 登录服务器，以下命令都在服务器终端运行。

## 1. 首次安装：复制命令，只改最后三项

```bash
curl -fsSL --connect-timeout 15 --max-time 120 --retry 3 https://github.com/yushengsheng/xiaojun-bn/releases/latest/download/install-linux.sh -o install-linux.sh && sudo bash install-linux.sh 你的公网IP 26483 '你的自定义密码'
```

| 要填写的内容 | 填法 |
| --- | --- |
| `你的公网IP` | 服务器公网 IPv4，也可以填完整域名 |
| `26483` | 你想使用的面板端口，1024–65535；不要使用内部端口 18880 |
| `'你的自定义密码'` | 换成自己的密码，至少 8 位；保留两侧单引号 |

例如：服务器地址为 `203.0.113.10`，想用 `28080` 端口，密码为 `MyPanel2026!`，最后一段就是：

```bash
sudo bash install-linux.sh 203.0.113.10 28080 'MyPanel2026!'
```

示例地址和密码仅用于说明，实际使用时替换为自己的。服务器的安全组和防火墙要放行 **TCP 80**（自动 HTTPS 证书验证）以及你填写的面板端口，例如 **TCP 28080**；内部端口 18880 不需要对公网开放。保留 TCP 80 供证书自动续期。

若提示找不到 `curl`，先执行一次：

```bash
sudo apt-get update && sudo apt-get install -y curl
```

命令中的 `sudo` 可能询问当前 Linux 用户的系统密码；它与面板密码分别设置。root 用户可以直接执行。

## 2. 安装完成后怎么打开

看到成功提示后，在浏览器打开：

```text
https://你的公网IP:你填写的端口/
```

用命令中填写的密码登录，**不需要用户名，也不需要初始化码**。随后在面板内添加交易所账号和设置策略；部署不会替你添加账号或自动开始交易。关闭浏览器不会停止服务器任务。

## 3. 以后升级，保留原密码

仍运行第一条下载命令，只将最后的密码替换为两个相邻的单引号 `''`。IP 和端口填写原来的值：

```bash
curl -fsSL --connect-timeout 15 --max-time 120 --retry 3 https://github.com/yushengsheng/xiaojun-bn/releases/latest/download/install-linux.sh -o install-linux.sh && sudo bash install-linux.sh 你的公网IP 26483 ''
```

脚本自动判断：没有安装则提示首次必须填密码；有新版且交易空闲则升级；已经最新则跳过，不重启服务。**空密码表示沿用原密码，不表示清空密码。** 账号、设置、证书和恢复记录保留。升级后重新登录，不会自动恢复交易。

## 4. 升级时同时换密码

将第三项填写为新密码：

```bash
sudo bash install-linux.sh 你的公网IP 26483 '你的新密码'
```

脚本会不回显地询问一次原程序密码。原密码用于解开原有加密资料，再用新密码保护同一份密钥，账号和恢复记录不变；忘记原密码时不能通过重装或替换密钥来保留原有资料。改密成功后旧密码失效，使用新密码登录。同版本也可以用此命令改密。

如果本地安装脚本尚未下载，先运行第 1 节完整下载命令。留空升级和改密都只在交易、查询、撤单和清理任务空闲时执行；忙碌时等待任务完成后重跑同一命令。

## 5. 常见提示

- **交易/清理仍在运行**：不强制停止，等待空闲后重跑。
- **端口占用**：新装时另选端口并在安全组放行，不关闭原服务。
- **证书申请失败**：检查 IP/域名、TCP 80 的安全组、防火墙和公网可达性。
- **原密码错误**：密码和加密资料保持原样；确认原密码后再运行。
- **未完成的安装/升级/改密标记**：保留现场和备份，先检查服务，勿删除账号、密钥或恢复记录后重装。
- **没有新版**：只使用已正式发布的 GitHub Release，源码提交尚未发版不会部署。

检查服务可复制：

```bash
sudo systemctl status xiaojun-exchange-worker xiaojun-exchange-web xiaojun-exchange-nginx --no-pager
```

升级至少需要 1 GiB 空闲磁盘。旧版本和私有部署备份保留在 `/opt/xiaojun-exchange/releases` 与 `/opt/xiaojun-exchange/upgrade-backups`，不会自动删除。

高级参数、自备证书和运行目录见 [Linux 运行说明](linux_exchange/README.md)。
