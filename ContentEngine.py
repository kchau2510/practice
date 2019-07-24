import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from scipy import sparse
from time import time
import logging

class ContentEngine():

    def __init__(self):
            self.log = logging.getLogger("content")
    def train(self):
        LocationItemRatings = pd.read_csv('data/user-item-location.csv', encoding='latin-1')
        
        Mean = LocationItemRatings.groupby(['location'], as_index= False, sort = False).mean().rename(columns={"location":"location", "product":"product", "rating":"rating_mean"})
        
        LocationItemRatings= pd.merge(LocationItemRatings, Mean, on='location', how="left", sort="False")
        
        LocationItemRatings['rating_adjusted'] = LocationItemRatings["rating"]- LocationItemRatings["rating_mean"]
        
        
        LocationItemRatingsAdjusted = pd.DataFrame({"location":LocationItemRatings['location'],
                               "product":LocationItemRatings['product_x'],
                               "rating":LocationItemRatings['rating_adjusted']})
        
        LocationItemPivot= LocationItemRatingsAdjusted.pivot_table(index ='location', columns ='product', values='rating').fillna(0)
        
        all_locations = LocationItemPivot.values
        A_sparse = sparse.csr_matrix(all_locations)
        start = time()
        similarities = cosine_similarity(A_sparse)
        self.log.info("finished kernel. this took {} s".format(time() - start))
        pd.DataFrame(similarities, index = LocationItemPivot.index, columns = LocationItemPivot.index).to_csv('data/location-location-data.csv')
        
engine = ContentEngine()
engine.train()