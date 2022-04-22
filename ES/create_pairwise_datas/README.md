# 利用ES的查询功能构建pair wise形式的训练数据集
## 1、输入数据格式
query,post,label,provice,ori <br />
我冲了10g流量 怎么没到账啊,充值流量未到账,1,湖北,查询流量 <br />
家庭v网流量余额查询,国内流量余额查询,-1,陕西,查流量 <br />

## 2、构建规则
如果label为1表示当前post为正列，query缺少一个负列，ES查询body为： <br />
body = '{"query":{"bool":{"must":[{"match":{"post":"%s"}}],"must_not": [{"term":{"originalPost.keyword":"%s"}}]}},"size":60}'%(post,ori)

如果label为-1表示当前post为负列，query缺少一个正列，ES查询body为：<br />
body = '{"query":{"bool":{"must":[{"match":{"post":"%s"}}, "term":{"originalPost.keyword":"%s"}}]}}}'%(post,ori)

## 3、输出数据格式
有两种形式：1）三列分别为query	pos_post	neg_post; 2)一列，每三行表示一个sample

