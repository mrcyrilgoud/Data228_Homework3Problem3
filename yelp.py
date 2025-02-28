from mrjob.job import MRJob
import json

class YelpAnalysis(MRJob):
    def mapper(self, _, line):
        try:
            review = json.loads(line)
            
            business_id = review.get('business_id', None)
            rating = review.get('stars', None)

            if business_id and rating is not None:
                if rating in [4, 5]:
                    yield business_id, ('positive', 1)
                elif rating in [1, 2]:
                    yield business_id, ('negative', 1)
                else:
                    yield business_id, ('neutral', 1)
        except Exception as e:
            pass

    def reducer(self, business_id, sentiment_counts):
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        
        for sentiment, count in sentiment_counts:
            if sentiment == 'positive':
                positive_count += count
            elif sentiment == 'negative':
                negative_count += count
            elif sentiment == 'neutral':
                neutral_count += count
        
        yield business_id, {'positive': positive_count, 'negative': negative_count, 'neutral': neutral_count}

if __name__ == '__main__':
    YelpAnalysis.run()
