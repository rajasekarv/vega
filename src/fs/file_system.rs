// WIP
pub trait FileSystem {
    fn get_default_port(&self) -> u16 {
        0
    }

    //    fn canonicalize_url(&self, mut url: Url) -> Url {
    //        if url.port().is_none() && self.get_default_port() > 0 {
    //            url.set_port(Some(self.get_default_port()));
    //        }
    //        return url;
    //    }
    //
    //    fn get_canonical_uri(&self) -> Url {
    //        self.canonicalize_url(self.get_uri().clone())
    //    }

    //fn get_uri(&self) -> &Url;

    //fn get_fs_of_path(Path abs_or_fq_path);
}
