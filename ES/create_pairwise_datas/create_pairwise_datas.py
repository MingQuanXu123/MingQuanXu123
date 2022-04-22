#encoding:utf-8
import io
import pycurl
import json,random
from datetime import datetime
import sys,gc
import pandas as pd
from tqdm import tqdm
import time

# provice到ES index之间的映射 
pro_index = {"江苏":"faq_jszx_sz", "山东":"faq_sdzx_sz", "福建":"faq_fjzx_sz", "湖北":"faq_hubeizx_sz", "浙江":"faq_zjzx_sz", "山西":"faq_sx035zx_sz", "云南":"faq_ynzx_sz", "江西":"faq_jxzx_sz", "陕西":"faq_sx029zx_sz", "河北":"faq_hebeizx_sz"}
# ES info
IP_PORT='192.168.98.203:9202'
PASSWD='test:123456'

def get_log_time():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')+':  '+__file__+':'

class Eshelper():
    def __init__(self,hosts,userpwd):
        self.hosts = hosts
        self.userpwd = userpwd
        self.timeout = 60
    
    def curlwrapper(self,url,postdata):
        s = io.BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL,url)
        #c.setopt(c.POSTFIELDS,postdata)
        postdata = postdata.encode('utf8')
        c.setopt(c.POSTFIELDS,postdata)
        c.setopt(c.VERBOSE,0)
        c.setopt(c.CONNECTTIMEOUT,self.timeout)
        c.setopt(c.TIMEOUT,self.timeout)
        c.setopt(c.USERPWD,self.userpwd)
        c.setopt(pycurl.WRITEFUNCTION,s.write)
        try:
            c.perform()
        except Exception as e:
            res = '{}'
            print(get_log_time()+"%s"%sys._getframe().f_lineno,e)
            print('='*100)
            return res
        res = s.getvalue()
        res = res.decode('utf-8')
        return res

    def get_all_data(self,index,postdata):
        result = []
        total = 0
        life = "2m"
        url = random.choice(self.hosts)
        url_ = '%s/%s/_search?scroll=%s'%(url,index,life)
        #print(url_,postdata)
        res = self.curlwrapper(url_,postdata)
        res = json.loads(res)
        if 'hits' not in res:
            print(get_log_time()+"%s"%sys._getframe().f_lineno,res)
            return []
        if 'hits' not in res['hits']:
            print(get_log_time()+"%s"%sys._getframe().f_lineno,res)
            return []
        result += res['hits']['hits']

        scroll_id = res["_scroll_id"]
        hitsize = len(res["hits"]["hits"])
        total += hitsize

        while(hitsize>0):
            url_ = '%s/_search/scroll'%url
            postdata = '{"scroll":"%s","scroll_id":"%s"}'%(life,scroll_id)
            res = self.curlwrapper(url_,postdata)
            res = json.loads(res)
            scroll_id = res.get("_scroll_id")
            if 'hits' not in res:
                print(get_log_time()+"%s"%sys._getframe().f_lineno,res)
                break
                #continue
            if 'hits' not in res['hits']:
                print(get_log_time()+"%s"%sys._getframe().f_lineno,res)
                continue
            result += res['hits']['hits']
            hitsize = len(res["hits"]["hits"])
            total += hitsize
        del res
        print(get_log_time()+"%s"%sys._getframe().f_lineno,'load es data done,data size is %s'%total)
        return result

    def get_single_data(self,index,postdata):
        url = random.choice(self.hosts)
        url_ = '%s/%s/_search'%(url,index)
        #print(url_)
        res = self.curlwrapper(url_,postdata)
        try:
            res = json.loads(res)
            result = res['hits']['hits']
        except:
            result = []
        return result

if __name__ == '__main__':
    es = Eshelper([IP_PORT],PASSWD)
    df = pd.read_csv("original_datas.csv")
    print(df.head()) 
    # 数据格式
    # query,post,label,provice,ori
    res = []
    res2 = []
    n = 0
    f = open('output.txt','w')
    for i in tqdm(df.index):
        n += 1
        query= str(df.loc[i,'query'])
        post = str(df.loc[i,'post'])
        ori = str(df.loc[i,'ori'])
        #构建负例
        label = str(df.loc[i, 'label'])
        provice = str(df.loc[i, 'provice'])
        index = pro_index[provice]
        if index == "":
            continue
        if(label== '1'):
            postdata = '{"query":{"bool":{"must":[{"match":{"post":"%s"}}],"must_not": \
                             [{"term":{"originalPost.keyword":"%s"}}]}}, "size":100}'%(post,ori)
            es_res = es.get_single_data(index,postdata)
            if len(es_res) == 0:
                f.write(query+','+ori+','+"label为1没有负列。"+'\n')
                continue
	    # 去三分之一 到 二分之一之间的负列
            for j in range(len(es_res)//3, len(es_res)//2):
                hits = es_res[j]
                post_neg = hits['_source']['post']
                
                line = []
                line.append(label)
                line.append(query)
                line.append(post)
                line.append(post_neg)

                res.append(line)

                res2.append(query)
                res2.append(post)
                res2.append(post_neg)


        if(label=='-1'):
        #构建正例
            postdata = '{"query":{"bool":{"must":[{"match":{"post":"%s"}}, \
                           {"term":{"originalPost.keyword":"%s"}}]}}, "size":100}'%(post,ori)
            es_res = es.get_single_data(index, postdata)
            if len(es_res) == 0:
                f.write(query+','+ori+','+"lebel为-1没有正列。"+'\n')
                continue

            for j in range(len(es_res)//3, len(es_res)//2):
                hits = es_res[j]
                post_pos = hits['_source']['post']
                
                line = []
                line.append(label)
                line.append(query)
                line.append(post_pos)
                line.append(post)
                
                res.append(line)

                res2.append(query)
                res2.append(post_pos)
                res2.append(post)

        if n%100 == 0:
            #break
            time.sleep(1)

    res = pd.DataFrame(res)
    res.columns=['label','query','post_pos','post_neg']
    res = res.sample(frac=1).reset_index(drop=True)
    res.to_csv('pairwise_results_4row.tsv',index=False)

    res2 = pd.DataFrame(res2)
    res2.columns=['query']
    res2 = res2.sample(frac=1).reset_index(drop=True)
    res2.to_csv('pairwise_results_1row.tsv',index=False)

    f.close()
    print("done")
