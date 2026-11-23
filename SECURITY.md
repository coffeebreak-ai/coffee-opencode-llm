# Reporting a vulnerability

English: do not open a public issue for a vulnerability or a leaked key. Contact the repository owner privately. The gateway is designed to run on one machine and listen on the loopback interface; it has no authentication, so do not expose its port to a network.

Русский: об уязвимости или об утёкшем ключе не открывайте публичную задачу. Напишите владельцу репозитория лично. Шлюз рассчитан на одну машину и слушает loopback; авторизации в нём нет, поэтому не публикуйте его порт в сеть.

简体中文：不要为漏洞或泄露的密钥开公开议题。请私下联系仓库所有者。网关设计为在单机上运行并监听回环地址；它没有认证，请不要把端口暴露到网络。

Include the commit version, what you did and what you expected. Send secrets through a separate private channel, never inside the issue text.
