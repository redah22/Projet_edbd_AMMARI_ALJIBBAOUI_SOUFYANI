------------------------------------------------------------
-- comment lancer le script
--
-- 1. créer la base de données :
--      createdb vinted
--
-- 2. exécuter le fichier sql :
--      psql -d vinted -f entrepot.sql
--
-- ce script crée toutes les dimensions, la table de faits,
-- la vue virtuelle, les vues matérialisées, les index
-- et les requêtes analytiques demandées dans le sujet.
------------------------------------------------------------



-- psql -d vinted
-- \d
-- \dm





------------------------------------------------------------
-- 0. nettoyage préalable
------------------------------------------------------------

drop materialized view if exists mv_ventes_article_date_visibilite cascade;
drop materialized view if exists mv_ventes_pays_transporteur_date cascade;
drop materialized view if exists mv_ventes_vendeur_pays_date cascade;

drop view if exists d_article cascade;

drop table if exists f_ventes cascade;
drop table if exists d_article_text cascade;
drop table if exists d_article_dynamiques cascade;
drop table if exists d_article_statiques cascade;
drop table if exists d_visibilite cascade;
drop table if exists d_livraison cascade;
drop table if exists d_date cascade;
drop table if exists d_utilisateur cascade;
drop table if exists d_localisation cascade;


------------------------------------------------------------
-- 1. dimensions
------------------------------------------------------------

create table d_localisation (
    id_localisation int primary key,
    pays            varchar(50),
    region          varchar(100),
    ville           varchar(100),
    code_postal     varchar(10),
    type_zone       varchar(50)
);

create table d_utilisateur (
    id_utilisateur                 int primary key,
    fk_localisation                int references d_localisation(id_localisation),
    pseudo                         varchar(100),
    date_inscription               date,
    anciennete_mois                int,
    nombre_total_ventes            int,
    nombre_total_achats            int,
    note_moyenne_acheteur          decimal(3,2),
    note_moyenne_vendeur           decimal(3,2),
    nb_evaluations_recues_acheteur int,
    nb_evaluations_recues_vendeur  int,
    statut_compte                  varchar(50),
    delai_envoi_moyen_historique   decimal(5,2)
);

create table d_date (
    id_date               int primary key,
    date_complete         date,
    annee                 int,
    trimestre             varchar(5),
    mois                  int,
    nom_mois              varchar(20),
    numero_semaine_annee  int,
    jour_du_mois          int,
    jour_semaine          varchar(20),
    est_weekend           boolean,
    saison                varchar(20)
);

create table d_livraison (
    id_livraison           int primary key,
    nom_transporteur       varchar(100),
    type_livraison         varchar(50),
    format_colis_attendu   varchar(20),
    est_international      boolean,
    option_suivi_inclus    boolean,
    delai_estime_min_jours int,
    delai_estime_max_jours int,
    zone_geographique      varchar(50),
    groupe_transporteur    varchar(50)
);

create table d_visibilite (
    id_visibilite             int primary key,
    type_service              varchar(50),
    nom_service               varchar(100),
    duree_service_jours       int,
    est_payant                boolean,
    description_service       text,
    niveau_priorite_affichage int,
    groupe_service            varchar(50),
    canal_affichage           varchar(50),
    code_reference_service    varchar(20)
);

create table d_article_statiques (
    id_article         int primary key,
    titre              varchar(200),
    marque             varchar(100),
    categorie          varchar(50),
    etat               varchar(50),
    matiere            varchar(50),
    couleur            varchar(50),
    taille             varchar(20),
    prix_initial       decimal(8,2),
    date_mise_en_ligne date
);

create table d_article_dynamiques (
    id_article                 int primary key references d_article_statiques(id_article),
    prix_actuel                decimal(8,2),
    statut                     varchar(50),
    visibilite_courante        varchar(50),
    nombre_vues                int,
    nombre_favoris             int,
    date_derniere_modification date
);

create table d_article_text (
    id_article  int primary key references d_article_statiques(id_article),
    description text,
    tags        text
);

------------------------------------------------------------
-- table pont entre articles et catégories
------------------------------------------------------------
CREATE TABLE d_categories (
    id_categorie SERIAL PRIMARY KEY,
    nom_categorie VARCHAR(100) NOT NULL,
    categorie_parent INTEGER,
    niveau_hierarchie INTEGER CHECK (niveau_hierarchie BETWEEN 1 AND 5),
    type_categorie VARCHAR(20) CHECK (type_categorie IN ('racine', 'parent', 'enfant')),
    FOREIGN KEY (categorie_parent) REFERENCES D_categories(id_categorie)
);

CREATE TABLE bridge_article_categorie (
    id_article INTEGER,
    id_categorie INTEGER,
    niveau_hierarchie INTEGER NOT NULL,
    PRIMARY KEY (id_article, id_categorie),
    FOREIGN KEY (id_article) REFERENCES D_article_statiques(id_article),
    FOREIGN KEY (id_categorie) REFERENCES D_categories(id_categorie)
);


------------------------------------------------------------
-- 2. vue virtuelle d_article
-- cette vue regroupe les trois tables article pour former
-- une seule dimension logique partagée dans l'entrepôt.
------------------------------------------------------------

create view d_article as
select
    s.id_article,
    s.titre,
    s.marque,
    s.categorie,
    s.etat,
    s.matiere,
    s.couleur,
    s.taille,
    s.prix_initial,
    s.date_mise_en_ligne,
    d.prix_actuel,
    d.statut,
    d.visibilite_courante,
    d.nombre_vues,
    d.nombre_favoris,
    d.date_derniere_modification,
    t.description,
    t.tags
from d_article_statiques s
join d_article_dynamiques d using (id_article)
join d_article_text t using (id_article);


------------------------------------------------------------
-- 3. table de faits f_ventes (modèle détaillé)
------------------------------------------------------------

create table f_ventes (
    id_vente                 int primary key,
    fk_vendeur               int references d_utilisateur(id_utilisateur),
    fk_acheteur              int references d_utilisateur(id_utilisateur),
    fk_article               int references d_article_statiques(id_article),
    fk_date_publication      int references d_date(id_date),
    fk_date_vente            int references d_date(id_date),
    fk_livraison             int references d_livraison(id_livraison),
    fk_visibilite            int references d_visibilite(id_visibilite),
    montant_vente            decimal(8,2),
    cout_option_visibilite   decimal(6,2),
    frais_protection         decimal(6,2),
    cout_livraison           decimal(6,2),
    duree_avant_vente_jours  int,
    nombre_vues_article      int,
    nombre_favoris           int,
    option_visibilite_active boolean
);


------------------------------------------------------------
-- 4. vues matérialisées (treillis d'agrégation)
-- ces vues pré-calculent des agrégations utilisées
-- par plusieurs requêtes analytiques.
------------------------------------------------------------

create materialized view mv_ventes_article_date_visibilite as
select
    a.categorie,
    a.etat,
    d.annee,
    v.type_service,
    count(*) as nb_ventes,
    sum(f.montant_vente) as ca_total,
    avg(f.duree_avant_vente_jours) as delai_moyen
from f_ventes f
join d_article_statiques a on f.fk_article = a.id_article
join d_date d on f.fk_date_vente = d.id_date
join d_visibilite v on f.fk_visibilite = v.id_visibilite
group by a.categorie, a.etat, d.annee, v.type_service;

create materialized view mv_ventes_pays_transporteur_date as
select
    loc.pays,
    l.nom_transporteur,
    d.annee,
    count(*) as nb_ventes,
    sum(f.montant_vente) as ca_total,
    avg(f.cout_livraison) as cout_moyen
from f_ventes f
join d_utilisateur u on f.fk_vendeur = u.id_utilisateur
join d_localisation loc on u.fk_localisation = loc.id_localisation
join d_livraison l on f.fk_livraison = l.id_livraison
join d_date d on f.fk_date_vente = d.id_date
group by loc.pays, l.nom_transporteur, d.annee;

create materialized view mv_ventes_vendeur_pays_date as
select
    u.id_utilisateur as id_vendeur,
    u.pseudo,
    loc.pays,
    d.annee,
    count(*) as nb_ventes,
    sum(f.montant_vente) as ca_total
from f_ventes f
join d_utilisateur u on f.fk_vendeur = u.id_utilisateur
join d_localisation loc on u.fk_localisation = loc.id_localisation
join d_date d on f.fk_date_vente = d.id_date
group by u.id_utilisateur, u.pseudo, loc.pays, d.annee;


------------------------------------------------------------
-- 5. index utilisés pour accélérer les requêtes analytiques
-- ÉQUIVALENT POSTGRESQL DES BITMAP JOIN INDEX :
-- PostgreSQL ne supporte pas les Bitmap Join Index comme Oracle. (et personne arrivait à utiliser oracle oups)
-- Alternative utilisée : index partiels + index composites.
--
-- INDEX OPTIMISANT SPÉCIFIQUEMENT 2 REQUÊTES :
-- 1. idx_fv_article + idx_art_cat → optimise requête 1 (CA par catégorie)
-- 2. idx_fv_livraison + idx_liv_trans → optimise requête 2 (transporteurs)
--
-- Les 9 autres index accélèrent les jointures générales.
------------------------------------------------------------

create index idx_fv_article on f_ventes(fk_article);
create index idx_fv_date on f_ventes(fk_date_vente);
create index idx_fv_visibilite on f_ventes(fk_visibilite);
create index idx_fv_livraison on f_ventes(fk_livraison);
create index idx_fv_vendeur on f_ventes(fk_vendeur);

create index idx_art_cat on d_article_statiques(categorie);
create index idx_art_etat on d_article_statiques(etat);

create index idx_date_annee on d_date(annee);
create index idx_vis_type on d_visibilite(type_service);
create index idx_loc_pays on d_localisation(pays);
create index idx_liv_trans on d_livraison(nom_transporteur);



















-- data set
------------------------------------------------------------
-- jeu de données de test pour vérifier les requêtes
------------------------------------------------------------

------------------------------------------------------------
-- insertion des localisations
------------------------------------------------------------
insert into d_localisation values
(1, 'France', 'Île-de-France', 'Paris', '75001', 'urbaine'),
(2, 'France', 'Auvergne-Rhône-Alpes', 'Lyon', '69000', 'urbaine'),
(3, 'Belgique', 'Bruxelles-Capitale', 'Bruxelles', '1000', 'urbaine'),
(4, 'Espagne', 'Catalogne', 'Barcelone', '08001', 'urbaine');

------------------------------------------------------------
-- insertion des utilisateurs
------------------------------------------------------------
insert into d_utilisateur values
(101, 1, 'VendeurParis', '2021-01-10', 36, 150, 40, 4.8, 4.7, 200, 180, 'actif', 2.5),
(102, 2, 'VendeurLyon',  '2022-05-12', 20, 90,  22, 4.5, 4.6, 120, 100, 'actif', 3.1),
(201, 3, 'AcheteurBE',   '2023-03-08', 12, 10,  35, 4.1, 4.9,  50, 45, 'actif', 0),
(202, 1, 'AcheteurFR',   '2023-06-01', 10, 5,   12, 4.0, 4.0,  20, 15, 'actif', 0);

------------------------------------------------------------
-- insertion des dates
------------------------------------------------------------
insert into d_date values
(20240110, '2024-01-10', 2024, 'T1', 1, 'janvier', 2, 10, 'mercredi', false, 'hiver'),
(20240111, '2024-01-11', 2024, 'T1', 1, 'janvier', 2, 11, 'jeudi',    false, 'hiver'),
(20240115, '2024-01-15', 2024, 'T1', 1, 'janvier', 3, 15, 'lundi',    false, 'hiver'),
(20240202, '2024-02-02', 2024, 'T1', 2, 'février', 5,  2, 'vendredi', false, 'hiver');

------------------------------------------------------------
-- insertion des modes de livraison
------------------------------------------------------------
insert into d_livraison values
(1, 'Mondial Relay',  'relais',  'S', false, true, 2, 4, 'France', 'MR'),
(2, 'Colissimo',       'domicile','M', false, true, 1, 3, 'France', 'LaPoste'),
(3, 'Chronopost',      'express', 'M', false, true, 1, 2, 'France', 'Express');

------------------------------------------------------------
-- insertion des visibilités
------------------------------------------------------------
insert into d_visibilite values
(1, 'standard',  'Standard', 0, false, 'visibilité normale', 1, 'base', 'flux', 'STD'),
(2, 'boost',     'Boost',    1, true,  'annonce mise en avant', 2, 'premium', 'flux', 'BST'),
(3, 'spotlight', 'Spotlight',5, true,  'plusieurs articles mis en avant', 3, 'premium', 'carrousel', 'SPT');

------------------------------------------------------------
-- insertion des articles (statiques)
------------------------------------------------------------
insert into d_article_statiques values
(1001, 'Robe été',      'Zara',     'robe',     'bon',       'coton',  'bleu',   'M', 15.00, '2024-01-01'),
(1002, 'Jean slim',     'Levis',    'jean',     'très bon',  'denim',  'noir',   'L', 25.00, '2024-01-05'),
(1003, 'Manteau hiver', 'Uniqlo',   'manteau',  'bon',       'laine',  'gris',   'M', 45.00, '2024-01-07'),
(1004, 'Sneakers',      'Nike',     'chaussure','excellent', 'cuir',   'blanc',  '42', 60.00, '2024-01-02');

------------------------------------------------------------
-- insertion des articles (dynamiques)
------------------------------------------------------------
insert into d_article_dynamiques values
(1001, 14.00, 'actif',   'standard', 120, 12, '2024-01-10'),
(1002, 22.00, 'vendu',   'boost',    200, 35, '2024-01-11'),
(1003, 40.00, 'actif',   'spotlight',150, 20, '2024-01-15'),
(1004, 55.00, 'vendu',   'boost',    300, 50, '2024-01-10');

------------------------------------------------------------
-- insertion des articles (textes)
------------------------------------------------------------
insert into d_article_text values
(1001, 'robe légère en coton bleu', 'été, robe, zara'),
(1002, 'jean slim noir levis',      'jean, levis'),
(1003, 'manteau chaud hiver',       'manteau, uniqlo'),
(1004, 'sneakers blanches nike',    'chaussure, nike');

------------------------------------------------------------
-- insertion de la table de faits (ventes)
------------------------------------------------------------
insert into f_ventes values
(1, 101, 201, 1001, 20240110, 20240115, 1, 1, 14.00, 0.00, 0.80, 3.50, 5, 120, 12, false),
(2, 101, 202, 1002, 20240111, 20240115, 2, 2, 22.00, 1.50, 0.80, 4.00, 4, 200, 35, true),
(3, 102, 201, 1003, 20240115, 20240202, 1, 3, 40.00, 2.00, 1.00, 5.00, 18,150, 20, true),
(4, 102, 202, 1004, 20240110, 20240111, 3, 2, 55.00, 1.50, 1.00, 6.00, 1, 300, 50, true),
(5, 101, 201, 1002, 20240110, 20240115, 1, 2, 22.00, 1.50, 0.80, 4.00, 5, 200, 35, true);


------------------------------------------------------------
-- insertion pour la table pont (hiérarchie catégories)
------------------------------------------------------------

INSERT INTO D_categories (id_categorie, nom_categorie, categorie_parent, niveau_hierarchie, type_categorie) VALUES 
(1, 'Femme', NULL, 1, 'racine'),
(2, 'Vêtements', 1, 2, 'parent'),
(3, 'Robes', 2, 3, 'enfant'),
(4, 'Jeans', 2, 3, 'enfant');

-- Lier les articles aux catégories
INSERT INTO bridge_article_categorie VALUES
(1001, 3, 3), (1001, 2, 2), (1001, 1, 1),  -- Robe liée à Robes > Vêtements > Femme
(1002, 4, 3), (1002, 2, 2), (1002, 1, 1);  -- Jean lié à Jeans > Vêtements > Femme



------------------------------------------------------------
-- 6. requêtes analytiques demandées dans le sujet
-- ces requêtes ne créent pas d'objet dans la base.
------------------------------------------------------------

-- requête pour analyser le chiffre d'affaires, le nombre de ventes
-- et le délai moyen selon la catégorie, l'année et le type de visibilité
select
    a.categorie,
    d.annee,
    v.type_service,
    count(*) as nb_ventes,
    sum(f.montant_vente) as chiffre_affaires,
    avg(f.duree_avant_vente_jours) as delai_moyen
from f_ventes f
join d_article_statiques a on f.fk_article = a.id_article
join d_date d on f.fk_date_vente = d.id_date
join d_visibilite v on f.fk_visibilite = v.id_visibilite
group by a.categorie, d.annee, v.type_service
order by chiffre_affaires desc;

-- requête pour comparer le volume de ventes et le chiffre d'affaires
-- selon les pays des vendeurs et les transporteurs
select
    loc.pays,
    l.nom_transporteur,
    d.annee,
    count(*) as nb_ventes,
    sum(f.montant_vente) as ca_total,
    avg(f.cout_livraison) as cout_moyen
from f_ventes f
join d_utilisateur u on f.fk_vendeur = u.id_utilisateur
join d_localisation loc on u.fk_localisation = loc.id_localisation
join d_livraison l on f.fk_livraison = l.id_livraison
join d_date d on f.fk_date_vente = d.id_date
group by loc.pays, l.nom_transporteur, d.annee
order by nb_ventes desc;

-- requête pour lister les vendeurs qui génèrent le plus de chiffre d'affaires
select
    u.id_utilisateur,
    u.pseudo,
    loc.pays,
    d.annee,
    count(*) as nb_ventes,
    sum(f.montant_vente) as chiffre_affaires
from f_ventes f
join d_utilisateur u on f.fk_vendeur = u.id_utilisateur
join d_localisation loc on u.fk_localisation = loc.id_localisation
join d_date d on f.fk_date_vente = d.id_date
group by u.id_utilisateur, u.pseudo, loc.pays, d.annee
order by chiffre_affaires desc
limit 10;

-- requête pour comparer le délai moyen de vente selon le type de visibilité
select
    v.type_service,
    count(*) as nb_ventes,
    avg(f.duree_avant_vente_jours) as delai_moyen
from f_ventes f
join d_visibilite v on f.fk_visibilite = v.id_visibilite
group by v.type_service
order by delai_moyen;

-- requête pour analyser les marques les plus rentables
select
    a.marque,
    count(*) as nb_ventes,
    sum(f.montant_vente) as ca_total,
    avg(f.montant_vente) as prix_moyen
from f_ventes f
join d_article_statiques a on f.fk_article = a.id_article
group by a.marque
having count(*) > 100
order by ca_total desc
limit 15;

-- requête pour mesurer le volume d'activité par pays et par année
select
    loc.pays,
    d.annee,
    count(*) as nb_ventes
from f_ventes f
join d_utilisateur u on f.fk_vendeur = u.id_utilisateur
join d_localisation loc on u.fk_localisation = loc.id_localisation
join d_date d on f.fk_date_vente = d.id_date
group by loc.pays, d.annee
order by nb_ventes desc;


------------------------------------------------------------
-- fin du fichier
------------------------------------------------------------
