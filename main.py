import pandas as pd
import numpy as np



def NettoyageDocumentAttendance(df) :

    #Création d'une colonne avec le nom d'équipe complet
    df['team_name_complet'] = df['team']+' '+df['team_name']
    colonne_a_supprimer = ['team', 'home', 'away', 'total']
    df = df.drop(columns=colonne_a_supprimer)

    #Passer les noms d'équipe en première position
    nouvel_ordre_colonnes = ['team_name_complet'] + [col for col in df.columns if col != 'team_name_complet']

    # Réorganiser les colonnes selon le nouvel ordre
    df = df.reindex(columns=nouvel_ordre_colonnes)

    #Passage en String des colonnes Year et Week pour la création de l'IDUnique
    df['year'] = df['year'].astype(str)
    df['week'] = df['week'].astype(str)

    #Création identifiant unique pour la jointure avec les autres fichiers
    df['IdMatch'] = df['team_name']+'Y'+df['year']+'W'+df['week']

    #Passage en int des colonnes Year et Week
    df['year'] = df['year'].astype(int)
    df['week'] = df['week'].astype(int)

    return df

def TraitementFichierGames(df) :
    #Dédoublement des lignes du DataFrame afin de pouvoir assigner un IDMatch à l'équipe à domicile et à l'extérieur

    df.reset_index(drop=True, inplace=True)
    df_double = pd.concat([df, df], axis=0, ignore_index=True)
    df_double.sort_index(inplace=True)

    return df_double

if __name__ == "__main__" :

    #Ouverture DataFrame avec le nombre de spectateur par match et par équipe
    df_attendance = pd.read_csv("data/attendance.csv")
    df_games = pd.read_csv("data/games.csv")
    df_standings = pd.read_csv("data/standings.csv")
    df_attendance = NettoyageDocumentAttendance(df_attendance)
    df_games = TraitementFichierGames(df_games)