from copy import copy
class PosTagger:

    def noun_tagger(self, nlp, story):
        doc = nlp(story)
        out = dict()
        for sent in doc.sentences:
            for token in sent.words:
                token_dict = out.get(token.text, dict())
                if token.upos in ["NOUN", "PROPN"]:
                    token_dict["POS"] = token.upos
                    out[token.text] = token_dict
        return copy(out)


    def is_noun(nouns, token_text):
        pos = nouns.get(token_text, None)
        if pos and pos.get("POS") == "NOUN":
            return True
        else:
            return False
