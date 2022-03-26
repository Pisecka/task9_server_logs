import os
import pandas as pd
import json
import argparse
import logging


#create logging
logging.basicConfig(level=logging.DEBUG)


# create argparse
parser = argparse.ArgumentParser(description='Getting filepath or directory')
parser.add_argument('--path', dest='path', default=os.getcwd(),
                    help='Enter filepath of log file or directory with log files')
args = parser.parse_args()

# check file or directory
if os.path.isfile(args.path):
    if not args.path.endswith('.log'):
        raise Exception('Not a log file')
    filepaths = [args.path]
    logging.info('File was given')
else:
    filepaths = [file for file in os.listdir(args.path) if file.endswith('.log')]
    if len(filepaths) == 0:
        raise Exception('Not a log files in this directory')

    logging.info('Directory was given')

for filepath in filepaths:

    def gen_from_file(filepath):
        '''
        create generator to read large file line by line
        :return:
        '''
        with open(filepath, 'r') as f:
            for line in f:
                yield line.split(maxsplit=10)

    gen = gen_from_file(filepath)

    try:
        logging.info('creating DataFrame')
        df = pd.DataFrame(gen)

        logging.info('Preprocessing Dataframe')
        #remove " from column with methods
        df[5] = df[5].apply(lambda x: x.replace('"',''))

        #remove empty columns 1,2
        df.drop([1,2], axis=1, inplace=True)

        # get duration columns
        df[11] = df[10].apply(lambda x: x.split()[-1].strip())

        # concat 2 columns with datetime
        df[3] = df[3] + df[4]
        df.drop(4, axis=1, inplace=True)

        # define column names
        df.columns = ['ip', 'time', 'method', 'url', 'protocol', 'response', 'received_bytes', 'user_agent', 'duration']

        # change received_bytes with "-" to 0
        df.loc[(df.received_bytes=='-'), 'received_bytes'] = 0

        # change type to integer
        df.response = df.response.astype(int)
        df.received_bytes = df.received_bytes.astype(int)
        df.duration = df.duration.astype(int)

        logging.info('Calculate count of requests')
        #общее количество выполненных запросов
        cnt_requests = df.shape[0]

        logging.info('Calculate count of HTTP requests')
        #количество запросов по HTTP-методам
        cnt_method_req =  df.method.value_counts().reset_index().rename(columns={'index':'method',
                                                                                 'method': 'cnt_requests'})

        logging.info('Calculate TOP3 IPs')
        #топ 3 IP адресов, с которых были сделаны запросы
        top3_ip =  df.ip.value_counts().head(3).reset_index().rename(columns={'index':'ip',
                                                                              'ip': 'cnt_requests'})

        logging.info('Calculate TOP3 long requests')
        #топ 3 самых долгих запросов, должно быть видно метод, url, ip, длительность, дату и время запроса
        top3_max_duration = df.sort_values(by='duration', ascending=False).head(3)[['method', 'url', 'ip', 'duration', 'time']]

        logging.info('Combine all data to dictionary')
        #create dict
        result = {}
        result['cnt_requests']=cnt_requests
        result['cnt_method_req'] = cnt_method_req.to_dict(orient='records')
        result['top3_ip'] = top3_ip.to_dict(orient='records')
        result['top3_max_duration'] = top3_max_duration.to_dict(orient='records')

        logging.info('Convert result to JSON')
        # convert dict to json
        print(json.dumps(result, indent=4))

        #save json to file
        filename = filepath.split('/')[-1].split('.')[0]
        with open(f'result_{filename}.json', 'w') as f:
            json.dump(result, fp=f, indent=4)

    except Exception as exc:
        print(f'ERROR: {exc}')



