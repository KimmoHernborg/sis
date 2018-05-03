#!/usr/bin/env python
from feature_extractor import FeatureExtractor
from PIL import Image
import glob
import os
import pickle
import queue
import random
import threading
import time

class Job:
    def __init__(self, image):
        self.image = image
        print('Job registered:', image)
        return

def construct_feature_path(img):
    fp = 'static/feature/' + os.path.splitext(os.path.basename(img))[0] + '.pkl'
    return fp

def process_job(q):
    process = True
    while True:
        job = q.get()
        print('Start:', job.image)
        try:
            img = Image.open(job.image)  # PIL image
            if process:
                if img.mode == 'CMYK':
                    img = img.convert('RGB')
                feature = fe.extract(img)
                feature_path = construct_feature_path(job.image)
                pickle.dump(feature, open(feature_path, 'wb'))
                img.save('static/img/' + os.path.splitext(os.path.basename(job.image))[0] + '.jpg') # Save jpg copy
        except OSError as e:
            print("*** ERROR opening:", job.image)
            print(e)
        except:
            raise
        print('Done: ', job.image)
        q.task_done()

# Setup the queue
q = queue.Queue()
for img_path in glob.iglob('static/img/*.tif'):
    if not os.path.isfile(construct_feature_path(img_path)):
        q.put(Job(img_path))

# Global Feature extractor
fe = FeatureExtractor()

workers = [
    threading.Thread(target=process_job, args=(q,)),
    threading.Thread(target=process_job, args=(q,)),
    threading.Thread(target=process_job, args=(q,)),
    threading.Thread(target=process_job, args=(q,)),
    threading.Thread(target=process_job, args=(q,)),
    threading.Thread(target=process_job, args=(q,)),
]
for w in workers:
    w.setDaemon(True)
    w.start()

q.join()
