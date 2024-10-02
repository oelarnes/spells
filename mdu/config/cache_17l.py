FILES = {
    'BLB': {
        'draft': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/draft_data/draft_data_public.BLB.PremierDraft.csv.gz",
            'target': "BLB_draft.csv"
        },
        'game': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/game_data/game_data_public.BLB.PremierDraft.csv.gz",
            'target': "BLB_game.csv"
        },
    },
    'MH3': {
        'draft': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/draft_data/draft_data_public.MH3.PremierDraft.csv.gz",
            'target': "MH3_draft.csv"
        },
        'game': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/game_data/game_data_public.MH3.PremierDraft.csv.gz",
            'target': "MH3_game.csv"
        }
    },
    'MKM': {
        'draft': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/draft_data/draft_data_public.MKM.PremierDraft.csv.gz",
            'target': "MKM_draft.csv"
        },
        'game': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/game_data/game_data_public.MKM.PremierDraft.csv.gz",
            'target': "MKM_game.csv"
        }
    },
    'OTJ': {
        'draft': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/draft_data/draft_data_public.OTJ.PremierDraft.csv.gz",
            'target': "OTJ_draft.csv"
        },
        'game': {
            'source': "https://17lands-public.s3.amazonaws.com/analysis_data/game_data/game_data_public.OTJ.PremierDraft.csv.gz",
            'target': "OTJ_game.csv"
        }
    },
}

DRAFT_SET_SYMBOL_MAP = {
    "BLB": ["blb", "spg"],
    "OTJ": ["otj", "otp", "big", "spg"]
}