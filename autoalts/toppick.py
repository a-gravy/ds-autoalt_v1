import logging
from autoalts.autoalt_maker import AutoAltMaker
from bpr.implicit_recommender import rerank
from autoalts.utils import file_to_list

logging.basicConfig(level=logging.INFO)


class TopPick(AutoAltMaker):
    def __init__(self, alt_info, create_date, blacklist_path, series_path=None, max_nb_reco=30, min_nb_reco=3):
        super().__init__(alt_info, create_date, blacklist_path, series_path, max_nb_reco, min_nb_reco)

    def make_alt(self, bpr_model_path=None, target_users_path=None, target_items_path=None):
        if self.alt_info['domain'].values[0] == "ippan_sakuhin":
            self.bpr_ippan(bpr_model_path, target_users_path, target_items_path)
        elif self.alt_info['domain'].values[0] == "semiadult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "adult":
            raise Exception("Not implemented yet")
        elif self.alt_info['domain'].values[0] == "book":
            raise Exception("Not implemented yet")
        else:
            raise Exception(f"unknown ALT_domain:{self.alt_info['domain'].values[0]}")

    def bpr_ippan(self, bpr_model_path, target_users_path=None, target_items_path=None):

        # loading things
        model = self.load_model(bpr_model_path)

        target_users = file_to_list(target_users_path) if target_users_path is not None else None
        target_items = file_to_list(target_items_path) if target_items_path is not None else None

        logging.info("make recommendation for {} users and {} items".format(
            len(target_users) if target_users else len(model.user_item_matrix.user2id),
            len(target_items) if target_items else len(model.user_item_matrix.item2id)
        ))

        with open(f"{self.alt_info['feature_public_code'].values[0]}.csv", 'w') as w:

            w.write(self.config['header']['autoalt'])

            for i, (userid, reranked_list, score_list) \
                    in enumerate(
                rerank(model=model, target_users=target_users, target_items=target_items,
                       filter_already_liked_items=True, N=800)):

                if i % 10000 == 0:
                    total_nb = len(target_users) if target_users else len(model.user_item_matrix.user2id)
                    logging.info("progress {}/{} = {:.1f}%".format(i, total_nb, float(i) / total_nb * 100))

                assert len(reranked_list) == len(score_list), "bug len(reranked_list):{} != len(score_list):{}".format(
                    len(reranked_list), len(score_list))

                reco = self.black_list_filtering(reranked_list)
                if self.series_dict:
                    reco = self.rm_series(reco)

                if len(reco) < self.min_nb_reco:
                    logging.info(f"{userid} has not enough recommendation contents")
                    continue

                w.write(
                    f"{userid},{self.alt_info['feature_public_code'].values[0]},{self.create_date},{'|'.join(reco[:self.max_nb_reco])},"
                    f"{self.alt_info['feature_title'].values[0]},{self.alt_info['domain'].values[0]},1\n")

                # TODO: solar format, gonna to remove it
                """
                sid_score = [(sid, score) for sid, score in zip(reranked_list, score_list) if
                             sid not in self.filter_items and sid not in set(self.dict_watched_sakuhin.get(userid, []))]

                
                r_score_list = ["{:.3f}".format(score) for _, score in sid_score]
                w.write(
                    "{},1,{},,,MF,MF,{}\n".format(userid, "|".join(r_reranked_list[:50]), "|".join(r_score_list[:50])))
                w.write(
                    "{},2,{},,,MF,MF,{}\n".format(userid, "|".join(r_reranked_list[50:100]),
                                                  "|".join(r_score_list[50:100])))
                """
