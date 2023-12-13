import pandas as pd
import numpy as np


def NettoyageDocumentAttendance(df):
    # Création d'une colonne avec le nom d'équipe complet
    df['team_name_complet'] = df['team'] + ' ' + df['team_name']
    colonne_a_supprimer = ['team', 'home', 'away', 'total']
    df = df.drop(columns=colonne_a_supprimer)

    # Passer les noms d'équipe en première position
    nouvel_ordre_colonnes = ['team_name_complet'] + [col for col in df.columns if col != 'team_name_complet']

    # Réorganiser les colonnes selon le nouvel ordre
    df = df.reindex(columns=nouvel_ordre_colonnes)

    # Passage en String des colonnes Year et Week pour la création de l'IDUnique
    df['year'] = df['year'].astype(str)
    df['week'] = df['week'].astype(str)

    # Création identifiant unique pour la jointure avec les autres fichiers
    df['IdMatch'] = df['team_name'] + 'Y' + df['year'] + 'W' + df['week']

    # Passage en int des colonnes Year et Week
    df['year'] = df['year'].astype(int)
    df['week'] = df['week'].astype(int)

    return df


def TraitementFichierGames(df):
    # Dédoublement des lignes du DataFrame avec la ligne doublé en dessous de la première afin de pouvoir assigner un IDMatch à l'équipe à domicile et à l'extérieur
    df_double = df._append(df)
    df_double = df_double.sort_values(by=['home_team', 'away_team', 'year', 'week'])
    df_double = df_double.reset_index(drop=True)

    # Création de la colonne IDMatch
    df_double['IdMatchHome'] = df_double['home_team_name'] + 'Y' + df_double['year'].astype(str) + 'W' + df_double[
        'week'].astype(str)

    # Création de la colonne IDMatchAway
    df_double['IdMatchAway'] = df_double['away_team_name'] + 'Y' + df_double['year'].astype(str) + 'W' + df_double[
        'week'].astype(str)

    # Mettre sur les lignes d'index pair home et pour les impairs away
    df_double['HomeAway'] = np.where(df_double.index % 2 == 0, 'Home', 'Away')

    return df_double


def CreationDataFrameHomeAway(df):
    df_home = df.loc[df['HomeAway'] == 'Home']
    df_away = df.loc[df['HomeAway'] == 'Away']

    return df_home, df_away


def TraitementFichierGamesAH(df_home, df_away, df_attendance):
    # Suppression des colonnes IdMatchHome dans le df_away et IdMatchAway dans le df_home
    df_home = df_home.drop(columns=['IdMatchAway'])
    df_away = df_away.drop(columns=['IdMatchHome'])

    # Jointure des deux dataframes df_home et df_away avec df_attendance
    df_home = df_home.merge(df_attendance, how='left', left_on='IdMatchHome', right_on='IdMatch')
    df_away = df_away.merge(df_attendance, how='left', left_on='IdMatchAway', right_on='IdMatch')


    return df_home, df_away


if __name__ == "__main__":
    # Ouverture DataFrame avec le nombre de spectateur par match et par équipe
    df_attendance = pd.read_csv("data/attendance.csv")
    df_games = pd.read_csv("data/games.csv")
    df_standings = pd.read_csv("data/standings.csv")
    df_attendance = NettoyageDocumentAttendance(df_attendance)
    df_games = TraitementFichierGames(df_games)
    df_games_home, df_games_away = CreationDataFrameHomeAway(df_games)
    df_games_home, df_games_away = TraitementFichierGamesAH(df_games_home, df_games_away, df_attendance)


