def _generate_recommendations(self,
                              target_users=None,  # target user index list in matrix; None means all users
                              target_items=None,
                              filter_items=None,
                              filter_already_liked_items=True,
                              N=200,
                              batch_size=500):
    since = time.time()
    ctr = 0
    n = len(target_users)
    if target_users is None:
        target_users = list(self.model.user_item_matrix.user2id.values())

    logger.info(f"Generating recommendations for {len(target_users)} users")

    # make target_matrix[nb of user, nb of target items], contains target items only by target_matrix_index
    if target_items is not None:
        target_items = list(set(target_items))
        target_items2actualid = {i: target for i, target in enumerate(target_items)}
        target_matrix = self.model.user_item_matrix.matrix[:, target_items]
        item_factors = self.model.item_factors[target_items, :]
    else:  # without target_items, the target_matrix is whole matrix
        target_matrix = self.model.user_item_matrix.matrix
        item_factors = self.model.item_factors
        target_items2actualid = {i: i for i in range(self.model.user_item_matrix.matrix.shape[1])}

    with open(self.out_path, 'w') as csvfile:
        with open('/app/bpr_score_all_new.csv', 'w') as csvfile_new:
            series_df = pd.read_csv(self.series_rel_path)
            series_dict = {}
            for row in series_df.iterrows():
                series_dict[row[1]["sakuhin_public_code"]] = row[1]["series_id"]
            del series_df

            for uidxs in batch(target_users, batch_size):

                # for uidxs(a batch of user), get the score for all items,
                # we don't use built func, it is for speeding up
                scores = self.model.user_factors[uidxs].dot(item_factors.T)
                rows = target_matrix[uidxs]

                for i, uid in enumerate(uidxs):

                    # TODO: Need to fix filter_already_liked_items
                    if filter_already_liked_items:
                        exclude = set(rows[i].nonzero()[1])
                    else:
                        exclude = set()

                    if filter_items:
                        exclude.update(filter_items)

                    # get the top N
                    count = max(N + len(exclude), 800)  # to make sure we have enough items for recommendation
                    if count < len(scores[i]):
                        ids = np.argpartition(scores[i], -count)[-count:]
                        best = sorted(zip(ids, scores[i][ids]), key=lambda x: -x[1])
                    else:
                        best = sorted(enumerate(scores[i]), key=lambda x: -x[1])

                    # get user id from index
                    user = self.model.user_item_matrix.id2user[uid]

                    series_recorder = set()
                    items = []
                    items_new = []

                    for i, rec in enumerate(best):
                        if len(items) >= N: break
                        if rec[0] not in exclude:
                            # get item id from index
                            itemid = target_items2actualid[rec[0]]

                            # keep the same series sakuhin only one in the list
                            # implement it here rather than "_squeeze_by_series" for memory efficiency
                            series_id = series_dict.get(self.model.user_item_matrix.id2item[itemid], None)
                            if series_id:
                                if series_id in series_recorder:
                                    continue
                                else:
                                    series_recorder.add(series_id)

                            items.append(f"{self.model.user_item_matrix.id2item[itemid]}:{float(rec[1])}")
                            items_new.append(f"{self.model.user_item_matrix.id2item[itemid]}:{float(rec[1])}")
                            csvfile.write("{},{},{:.3f}\n".format(user, self.model.user_item_matrix.id2item[itemid],
                                                                  float(rec[1])))
                    csvfile_new.write(f'{user},{"|".join(items_new)}\n')
                    logging.debug("for user:{}  {} items are recommended".format(user, len(items)))

                ctr = ctr + len(uidxs)
                elapsed = time.time() - since
                minutes, second = round(elapsed // 60, 4), round(elapsed % 60, 4)
                if not self.quiet:
                    logger.info("{}/{}, exec time={}:{:.4f}".format(ctr, n, int(minutes), second))

        logger.info(f"Generated the final recommendation file to {self.out_path}")
        logger.info(f"Generated the final recommendation file to /app/bpr_score_all_new.csv")
