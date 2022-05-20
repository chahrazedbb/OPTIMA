import pandas as pd

def load_dataset(dir_path):
    #adding dictionnary of dataset information (integer encoding)
    data = dict()
    data["person"] = pd.read_csv(dir_path + '/person.csv', header=None)
    data["review"] = pd.read_csv(dir_path + '/review.csv', header=None)
    data["offer"] = pd.read_csv(dir_path + '/offer.csv', header=None)
    data["product"] = pd.read_csv(dir_path + '/product.csv', header=None)
    data["producer"] = pd.read_csv(dir_path + '/producer.csv', header=None)

    person_column = {
        'nr':0,'name':1,'mbox_sha1sum':2,'personCountry':3,'personPublisher':4,'personPublishDate':5
    }
    review_column = {
        'id': 0, 'product': 1, 'producer': 2, 'person': 3, 'reviewDate': 4,'title':5,'text':6,'language':7,'rating1':8,'rating2':9,'rating3':10,'rating4':11,'reviewPublisher':12,'reviewPublishDate':13
    }
    offer_column = {
        'id':0,'product':1,'producer':2,'vendor':3,'price':4,'validFrom':5,'validTo':6,'deliveryDays':7,'offerWebpage': 8, 'publisher': 9, 'publishDate': 10
    }
    product_column = {
        'id':0,'label':1,'comment':2,'producer':3,'propertyNum1':4,'propertyNum2':5,'propertyNum3':6,'propertyNum4':7,'propertyNum5':8,'propertyNum6':9,'propertyTex1':10,'propertyTex2':11,'propertyTex3':12,'propertyTex4':13,'propertyTex5':14,'propertyTex6':15,'productPublisher':16,'productPublishDate':17

    }
    producer_column = {
        'id':0,'producerLabel':1,'producerComment':2,'homepage':3,'producerCountry':4,'producerPublisher':5,'producerPublishDate':6
    }


    data["person"].columns = person_column
    data["review"].columns = review_column
    data["product"].columns = product_column
    data["producer"].columns = producer_column
    data["offer"].columns = offer_column
    return data
