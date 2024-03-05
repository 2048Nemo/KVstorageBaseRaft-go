> 本项目的目的是学习Raft算法，并实现一个简单的k-v存储数据库。因此并不适用于生产环境

## 目录说明：

- temp ：保存持久化KVserver 存储的数据，和保存数据库初始化用的文件
- kvnode ： raft 核心算法实现的目录
- rpc： 定义了raft rpc通信的数据结构
- kvserver： 将kvnode 实现实现的核心封装成了kv服务器
## 快速理清调用顺序：
[https://www.yuque.com/keqinkejian/tdcp7g/ge8pi8qakprnqo6l?singleDoc#](https://www.yuque.com/keqinkejian/tdcp7g/ge8pi8qakprnqo6l?singleDoc#) 《raft算法调用示例图》
## 运行截图：
![image.png](https://cdn.nlark.com/yuque/0/2024/png/28177473/1709561930452-b779acda-c7b1-47de-b8ce-87bc0cdcdd77.png#averageHue=%231f2125&clientId=u6511d1e7-4224-4&from=paste&height=815&id=u001109f6&originHeight=815&originWidth=1804&originalType=binary&ratio=1&rotation=0&showTitle=false&size=142868&status=done&style=none&taskId=uc2920d24-b4ca-4830-bfe8-e5905053595&title=&width=1804)![image.png](https://cdn.nlark.com/yuque/0/2024/png/28177473/1709561926339-96bc10c8-8d95-47c3-8041-958744f8dd02.png#averageHue=%231f2125&clientId=u6511d1e7-4224-4&from=paste&height=811&id=uac6aec24&originHeight=811&originWidth=1792&originalType=binary&ratio=1&rotation=0&showTitle=false&size=145360&status=done&style=none&taskId=uc4dc5942-e87b-41a1-8c59-f81ff90040b&title=&width=1792)
## 资料整理：

- [根据raft算法的论文中实现的结构和调用细节（自己写的话没必要严格参考论文的所有字段）](https://zhuanlan.zhihu.com/p/84587451)
- [投票选主细节](https://zhuanlan.zhihu.com/p/541515590)
- [快速了解什么是raft算法](https://developer.aliyun.com/article/846860#:~:text=%E7%AE%80%E4%BB%8B%EF%BC%9ARaft%E6%98%AF,%E6%95%B0%E6%8D%AE%E8%BE%BE%E6%88%90%E4%B8%80%E8%87%B4%E6%80%A7%E3%80%82)
- [raft动画，强烈建议刚刚才接触raft算法的同学看一下这个动画（最好稍微理解一些理论再看动画会更加透彻）](https://thesecretlivesofdata.com/raft/?spm=a2c6h.12873639.article-detail.7.34cd5de3oKXfw2)
> 另外很多人想看一下raft的论文但是苦于英语不太好，我这边也附上我在学习过程中用翻译狗机器翻译过的版本，虽然有些机翻的地方很难看，但是也总比直接看纯英文好些，另外我才用的是分段翻译，觉得机翻翻译的不好可以中英文对照着看。ps：机器翻译把图片都给毁了，想看图片还是直接看raft原文吧

- [raft【翻译狗www.fanyigou.com】.pdf](https://www.yuque.com/attachments/yuque/0/2024/pdf/28177473/1709562794104-bd7be836-aca4-434b-8db7-5c780b03d62f.pdf?_lake_card=%7B%22src%22%3A%22https%3A%2F%2Fwww.yuque.com%2Fattachments%2Fyuque%2F0%2F2024%2Fpdf%2F28177473%2F1709562794104-bd7be836-aca4-434b-8db7-5c780b03d62f.pdf%22%2C%22name%22%3A%22raft%E3%80%90%E7%BF%BB%E8%AF%91%E7%8B%97www.fanyigou.com%E3%80%91.pdf%22%2C%22size%22%3A4623157%2C%22ext%22%3A%22pdf%22%2C%22source%22%3A%22%22%2C%22status%22%3A%22done%22%2C%22download%22%3Atrue%2C%22taskId%22%3A%22uabbeace9-b221-40ed-94b8-7878b467ed4%22%2C%22taskType%22%3A%22upload%22%2C%22type%22%3A%22application%2Fpdf%22%2C%22__spacing%22%3A%22both%22%2C%22mode%22%3A%22title%22%2C%22id%22%3A%22u35875aa6%22%2C%22margin%22%3A%7B%22top%22%3Atrue%2C%22bottom%22%3Atrue%7D%2C%22card%22%3A%22file%22%7D)
- [raft.pdf](https://www.yuque.com/attachments/yuque/0/2024/pdf/28177473/1709562791185-fe951bc3-6122-49fb-96bc-c496b1f81f10.pdf?_lake_card=%7B%22src%22%3A%22https%3A%2F%2Fwww.yuque.com%2Fattachments%2Fyuque%2F0%2F2024%2Fpdf%2F28177473%2F1709562791185-fe951bc3-6122-49fb-96bc-c496b1f81f10.pdf%22%2C%22name%22%3A%22raft.pdf%22%2C%22size%22%3A567533%2C%22ext%22%3A%22pdf%22%2C%22source%22%3A%22%22%2C%22status%22%3A%22done%22%2C%22download%22%3Atrue%2C%22taskId%22%3A%22ubc5b4b8d-6aa0-41dd-98fb-b5d4631ff18%22%2C%22taskType%22%3A%22upload%22%2C%22type%22%3A%22application%2Fpdf%22%2C%22__spacing%22%3A%22both%22%2C%22mode%22%3A%22title%22%2C%22id%22%3A%22ue59783d2%22%2C%22margin%22%3A%7B%22top%22%3Atrue%2C%22bottom%22%3Atrue%7D%2C%22card%22%3A%22file%22%7D)
> 另外如果您发现本文存在链接失效请访问：[KVStorageBaseRaft-go](#%20%E3%80%8AKVStorageBaseRaft-go%E3%80%8B)

